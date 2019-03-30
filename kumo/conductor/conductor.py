import abc
import os
import json
import subprocess
import time
import tarfile
import boto3
import requests
import math
import datetime
from functools import wraps

from azure.storage import blob
from azure.mgmt import compute
from azure.mgmt import network
from azure.common.client_factory import get_client_from_json_dict

from google.cloud import storage
from google.cloud import exceptions
from google.oauth2 import service_account

from googleapiclient import discovery

from celery import Celery
from celery import task

CELERY = Celery()
CELERY.config_from_envvar("CELERY_BROKER_URL", "amqp://")


def audit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        initial = datetime.datetime.now()
        print("{}:{}:{}:started".format(
            args[0].__class__.__name__, func.__name__, initial))
        result = func(*args, **kwargs)
        final = datetime.datetime.now()
        print("{}:{}:{}:ended".format(
            args[0].__class__.__name__, func.__name__, final))
        total = final - initial
        print("{}:{}:{}:total".format(
            args[0].__class__.__name__, func.__name__, total.total_seconds()))
        return result
    return wrapper


def get_assume_role_policy_document():
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "vmie.amazonaws.com"},
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": "vmimport"
                    }
                }
            }
        ]
    })


def get_policy_document(bucket):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::{0}".format(bucket),
                    "arn:aws:s3:::{0}/*".format(bucket)
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:ModifySnapshotAttribute",
                    "ec2:CopySnapshot",
                    "ec2:RegisterImage",
                    "ec2:Describe*"
                ],
                "Resource":"*"
            }
        ]
    })


class KumoException(Exception):
    pass


class BaseDriver(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def create_bucket(self):
        pass

    @abc.abstractmethod
    def stop_server(self):
        pass

    @abc.abstractmethod
    def export_disk(self):
        pass

    @abc.abstractmethod
    def download_disk(self):
        pass

    @abc.abstractmethod
    def prepare_disk(self):
        pass

    @abc.abstractmethod
    def upload_disk(self):
        pass

    @abc.abstractmethod
    def import_disk(self):
        pass

    @abc.abstractmethod
    def create_server(self):
        pass


class AmazonDriver(BaseDriver):

    def __init__(self, source_or_destination, migration):
        self.server_name = migration.get("virtual_machine")
        migration = migration.get(source_or_destination)
        self.bucket_name = migration.get("bucket")
        self.server_capacity = migration.get("instance_type")
        self.cloud_region = migration.get("region")
        self.cloud_availability_zone = migration.get("availability_zone")
        self.aws_access_key_id = migration.get("aws_access_key_id")
        self.aws_secret_access_key = migration.get("aws_secret_access_key")

    @audit
    def start_server(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)

        response = ec2.describe_instances(
                Filters=[{"Name": "tag:Name", "Values": [self.server_name]}])
        reservations = response.get("Reservations")
        instances = reservations[0].get("Instances")
        if instances:
            instance = instances[0]
            instance_id = instance.get("InstanceId")
            response = ec2.start_instances(InstanceIds=[instance_id])
            waiter = ec2.get_waiter("instance_running")
            waiter.wait(
                Filters=[{"Name": "instance-id", "Values": [instance_id]}],
                WaiterConfig={"Delay": 1, "MaxAttempts": 600})

    @audit
    def delete_image(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)
        images = ec2.describe_images(
            Filters=[{"Name": "name", "Values": [self.server_name]}])
        image = images.get("Images")
        if image:
            image = images.get("Images")[0]
            image_id = image.get("ImageId")
            response = ec2.deregister_image(ImageId=image_id)

    @audit
    def delete_bucket(self):
        iam = boto3.client(
            "iam",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        resource = boto3.resource(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)
        try:
            iam.delete_role_policy(PolicyName="vmimport", RoleName="vmimport")
        except Exception:
            pass

        try:
            iam.delete_role(RoleName="vmimport")
        except Exception:
            pass

        try:
            bucket = resource.Bucket(self.bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except Exception:
            pass

    @audit
    def delete_server(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)
        response = ec2.describe_instances(
                Filters=[
                    {"Name": "instance-state-name", "Values": ["running"]},
                    {"Name": "tag:Name", "Values": [self.server_name]}])
        reservations = response.get("Reservations")
        instances = reservations[0].get("Instances")
        if instances:
            instance = instances[0]
            instance_id = instance.get("InstanceId")
            ec2.terminate_instances(InstanceIds=[instance_id])
            waiter = ec2.get_waiter("instance_terminated")
            waiter.wait(
                Filters=[{"Name": "instance-id", "Values": [instance_id]}],
                WaiterConfig={"Delay": 1, "MaxAttempts": 600})

    @audit
    def create_bucket(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

        try:
            s3.head_bucket(Bucket=self.bucket_name)
        except Exception:
            try:
                if(self.cloud_region == "us-east-1"):
                    s3.create_bucket(Bucket=self.bucket_name)
                else:
                    s3.create_bucket(Bucket=self.bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": self.cloud_region})
                waiter = s3.get_waiter("bucket_exists")
                waiter.wait(
                    Bucket=self.bucket_name,
                    WaiterConfig={"Delay": 1, "MaxAttempts": 600})
            except Exception:
                raise KumoException("Error while creating bucket on aws.")

        bucket_acl = s3.get_bucket_acl(Bucket=self.bucket_name)
        owner_id = bucket_acl.get("Owner").get("ID")
        grant_full_controll = "id={0}".format(owner_id)

        try:
            response = s3.put_bucket_acl(
                Bucket=self.bucket_name,
                GrantFullControl=grant_full_controll,
                GrantWrite="emailaddress=vm-import-export@amazon.com",
                GrantReadACP="emailaddress=vm-import-export@amazon.com")
        except Exception:
            raise KumoException("Error while creating acl for bucket on aws s3.")

        iam = boto3.client(
            "iam",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

        try:
            iam.get_role(RoleName="vmimport")
        except Exception:
            try:
                iam.create_role(
                    AssumeRolePolicyDocument=get_assume_role_policy_document(),
                    RoleName="vmimport")
            except Exception:
                raise KumoException("Error while creating vmimport role on aws iam.")

        try:
            iam.get_role_policy(
                RoleName="vmimport",
                PolicyName="vmimport")
        except Exception:
            try:
                iam.put_role_policy(
                    RoleName="vmimport",
                    PolicyName="vmimport",
                    PolicyDocument=get_policy_document(self.bucket_name))
            except Exception:
                raise KumoException("Error while creating vmimport policy on aws iam.")

    @audit
    def stop_server(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)
        response = ec2.describe_instances(
                Filters=[{"Name": "tag:Name", "Values": [self.server_name]}])
        reservations = response.get("Reservations")
        instances = reservations[0].get("Instances")
        if instances:
            instance = instances[0]
            instance_id = instance.get("InstanceId")
            ec2.stop_instances(InstanceIds=[instance_id])
            waiter = ec2.get_waiter("instance_stopped")
            waiter.wait(
                Filters=[{"Name": "instance-id", "Values": [instance_id]}],
                WaiterConfig={"Delay": 1, "MaxAttempts": 600})

    @audit
    def export_disk(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)

        export_to_s3_task = {
            "DiskImageFormat": "VHD",
            "S3Bucket": self.bucket_name
        }

        response = ec2.describe_instances(
                Filters=[{"Name": "tag:Name", "Values": [self.server_name]}])
        reservations = response.get("Reservations")
        instances = reservations[0].get("Instances")
        if instances:
            instance = instances[0]
            instance_id = instance.get("InstanceId")

        try:
            response = ec2.create_instance_export_task(
                ExportToS3Task=export_to_s3_task,
                InstanceId=instance_id,
                TargetEnvironment="microsoft")
        except Exception:
            raise KumoException("Error while create export task on aws ec2.")

        export_task = response.get("ExportTask")
        export_task_id = export_task.get("ExportTaskId")

        waiter = ec2.get_waiter("export_task_completed")

        waiter.wait(
            ExportTaskIds=[export_task_id],
            WaiterConfig={"Delay": 1, "MaxAttempts": 43200})

        export_to_s3_task = export_task.get("ExportToS3Task")
        self.aws_s3_disk_name = export_to_s3_task.get("S3Key")

    @audit
    def download_disk(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

        key = "{0}{1}{2}".format(self.server_name, ".", "vhd")

        try:
            s3.download_file(
                Bucket=self.bucket_name,
                Key=self.aws_s3_disk_name,
                Filename="/home/ubuntu/volume/{0}".format(key))
        except Exception:
            raise KumoException("Error while downloading file from aws s3.")

        disk = "/home/ubuntu/volume/{0}".format(key)
        print("disk size (bytes): {}".format(os.path.getsize(disk)))

    @audit
    def prepare_disk(self):
        pass

    @audit
    def upload_disk(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key)

        blob_name = "{0}.{1}".format(self.server_name, "vhd")
        path = os.path.join("/home/ubuntu/volume/{0}".format(blob_name))

        with open(path, "rb") as data:
            s3.upload_fileobj(data, self.bucket_name, blob_name)
        disk = "/home/ubuntu/volume/{0}".format(blob_name)
        print("disk size (bytes): {}".format(os.path.getsize(disk)))
        os.remove(disk)
        print("disk status: deleted")

    @audit
    def import_disk(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)

        disk_containers = [{
            "Format": "VHD",
            "UserBucket": {
                "S3Bucket": self.bucket_name,
                "S3Key": "{0}.{1}".format(self.server_name, "vhd")
            }
        }]

        try:
            response = ec2.import_image(DiskContainers=disk_containers)
        except Exception:
            raise KumoException("Error while importing image to aws ec2.")

        import_task_id = response.get("ImportTaskId")

        for second in range(43200):
            time.sleep(1)
            response = ec2.describe_import_image_tasks(ImportTaskIds=[import_task_id])
            import_image_task = response.get("ImportImageTasks")[0]
            status = import_image_task.get("Status")
            status_message = import_image_task.get("StatusMessage")
            if second % 60 == 0 and status_message:
                print("task {} status: {}".format(import_task_id, status_message))
            if status == "completed":
                print("task {} status: completed".format(import_task_id))
                break

        self.aws_ec2_image_id = import_image_task.get("ImageId")

        waiter = ec2.get_waiter("image_available")
        waiter.wait(
            Filters=[{"Name": "image-id", "Values": [self.aws_ec2_image_id]}],
            WaiterConfig={"Delay": 1, "MaxAttempts": 6000})

    @audit
    def create_server(self):
        ec2 = boto3.client(
            "ec2",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.cloud_region)

        tag_specifications = [
            {"ResourceType": "instance",
             "Tags": [{"Key": "Name", "Value": self.server_name}]}]

        response = ec2.run_instances(
            MaxCount=1,
            MinCount=1,
            InstanceType=self.server_capacity,
            ImageId=self.aws_ec2_image_id,
            TagSpecifications=tag_specifications,
            Placement={"AvailabilityZone": self.cloud_availability_zone})
        instances = response.get("Instances")
        instance = instances[0]
        instance_id = instance.get("InstanceId")
        waiter = ec2.get_waiter("instance_running")
        waiter.wait(
            Filters=[{"Name": "instance-id", "Values": [instance_id]}],
            WaiterConfig={"Delay": 1, "MaxAttempts": 6000})


class GoogleDriver(BaseDriver):

    def __init__(self, source_or_destination, migration):
        self.server_name = migration.get("virtual_machine")
        migration = migration.get(source_or_destination)
        self.bucket_name = migration.get("bucket")
        self.disk_system = migration.get("system")
        self.server_capacity = migration.get("machine_type")
        self.cloud_zone = migration.get("zone")
        self.credentials = {}
        self.credentials["type"] = migration.get("type")
        self.credentials["project_id"] = migration.get("project_id")
        self.credentials["private_key_id"] = migration.get("private_key_id")
        self.credentials["client_email"] = migration.get("client_email")
        self.credentials["client_id"] = migration.get("client_id")
        self.credentials["auth_uri"] = migration.get("auth_uri")
        self.credentials["token_uri"] = migration.get("token_uri")
        self.credentials["auth_provider_x509_cert_url"] = migration.get("auth_provider_x509_cert_url")
        self.credentials["client_x509_cert_url"] = migration.get("client_x509_cert_url")
        self.credentials["private_key"] = migration.get("private_key")
        self.project_id = self.credentials["project_id"]
        self.client_email = self.credentials["client_email"]

    @audit
    def delete_server(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gce = discovery.build("compute", "v1", credentials=credentials)
        gce.instances().delete(
            project=self.project_id,
            zone=self.cloud_zone,
            instance=self.server_name).execute()

        for _ in range(43200):
            time.sleep(1)
            response = gce.instances().get(
                project=self.project_id,
                zone=self.cloud_zone,
                instance=self.server_name).execute()
            status = response.get("status")
            if status == "TERMINATED":
                break
    @audit
    def start_server(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gce = discovery.build("compute", "v1", credentials=credentials)
        gce.instances().start(
            project=self.project_id,
            zone=self.cloud_zone,
            instance=self.server_name).execute()

        for _ in range(43200):
            time.sleep(1)
            response = gce.instances().get(
                project=self.project_id,
                zone=self.cloud_zone,
                instance=self.server_name).execute()
            status = response.get("status")
            if status == "RUNNING":
                break

    @audit
    def delete_image(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gce = discovery.build("compute", "v1", credentials=credentials)
        try:
            gce.images().delete(
                project=self.project_id,
                image=self.server_name).execute()
        except Exception:
            pass

    @audit
    def delete_bucket(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gcs = storage.Client(project=self.project_id, credentials=credentials)
        try:
            bucket = gcs.get_bucket(bucket_or_name=self.bucket_name)
            blobs = bucket.list_blobs()
            for blob in blobs:
                blob.delete()
            bucket.delete()
        except exceptions.NotFound:
            pass

    @audit
    def create_bucket(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gcs = storage.Client(project=self.project_id, credentials=credentials)

        try:
            gcs.get_bucket(bucket_or_name=self.bucket_name)
        except exceptions.NotFound:

            bucket = gcs.bucket(self.bucket_name)
            try:
                bucket.create()
            except Exception:
                raise KumoException("Error while create bucket on gcp gcs.")

    @audit
    def stop_server(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gce = discovery.build("compute", "v1", credentials=credentials)

        response = gce.instances().stop(
            project=self.project_id,
            zone=self.cloud_zone,
            instance=self.server_name).execute()

        for _ in range(600):
            time.sleep(1)
            response = gce.instances().get(
                project=self.project_id,
                zone=self.cloud_zone,
                instance=self.server_name).execute()
            status = response.get("status")
            if status == "TERMINATED":
                break

    @audit
    def export_disk(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gce = discovery.build("compute", "v1", credentials=credentials)

        response = gce.instances().get(
            project=self.project_id,
            zone=self.cloud_zone,
            instance=self.server_name).execute()

        disk = response["disks"][0]["source"].split("/")[-1]

        disk = "projects/{0}/zones/{1}/disks/{2}".format(self.project_id, self.cloud_zone, disk)

        body = {"name": self.server_name, "sourceDisk": disk}

        response = gce.images().insert(
            project=self.project_id,
            body=body).execute()

        for _ in range(43200):
            time.sleep(1)
            response = gce.images().get(
                project=self.project_id,
                image=self.server_name).execute()
            status = response.get("status")
            if status == "READY":
                break

        home = "/home/ubuntu/volume"
        home = os.path.join(home, "{0}{1}{2}".format(self.project_id, ".", "json"))

        with open(home, "w") as credentials_file:
            json.dump(self.credentials, credentials_file)

        key_file = "--key-file={0}".format(home)
        project = "--project={0}".format(self.project_id)

        result = subprocess.run(
            ["gcloud", "auth", "activate-service-account",
             self.client_email, key_file, project], stdout=subprocess.PIPE)

        image = "--image={0}".format(self.server_name)
        destination_uri = "--destination-uri=gs://{0}/{1}.{2}".format(
            self.bucket_name, self.server_name, "vhd")

        command = ["gcloud", "compute", "images", "export",
                   image, destination_uri]

        export_format = "--export-format={0}".format("vpc")
        command.append(export_format)

        result = subprocess.run(command, stdout=subprocess.PIPE)

    @audit
    def download_disk(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gcs = storage.Client(project=self.project_id, credentials=credentials)
        bucket = gcs.get_bucket(bucket_or_name=self.bucket_name)
        blob_name = "{0}.{1}".format(self.server_name, "vhd")
        blob = bucket.blob(blob_name=blob_name)
        path = "/home/ubuntu/volume"
        path = os.path.join(path, "{0}".format(blob_name))
        blob.download_to_filename(path)
        disk = "/home/ubuntu/volume/{0}".format(blob_name)
        print("disk size (bytes): {}".format(os.path.getsize(disk)))

    @audit
    def prepare_disk(self):
        pass

    @audit
    def upload_disk(self):
        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gcs = storage.Client(project=self.project_id, credentials=credentials)

        try:
            bucket = gcs.get_bucket(bucket_or_name=self.bucket_name)
        except TypeError:
            raise KumoException("Error while get bucket on gcp gcs.")

        blob_name = "{0}{1}{2}".format(self.server_name, ".", "vhd")
        blob = bucket.blob(blob_name=blob_name)
        filename = "/home/ubuntu/volume/{0}".format(blob_name)
        try:
            blob.upload_from_filename(filename=filename)
        except TypeError:
            raise KumoException("Error while create bucket on gcp gcs.")

        disk = "/home/ubuntu/volume/{0}".format(blob_name)
        print("disk size (bytes): {}".format(os.path.getsize(disk)))
        os.remove(disk)
        print("disk status: deleted")

    @audit
    def import_disk(self):
        home = "/home/ubuntu/volume"
        home = os.path.join(home, "{0}{1}{2}".format(self.project_id, ".", "json"))

        with open(home, "w") as credentials_file:
            json.dump(self.credentials, credentials_file)

        key_file = "--key-file={0}".format(home)
        project = "--project={0}".format(self.project_id)

        result = subprocess.run(
            ["gcloud", "auth", "activate-service-account",
             self.client_email, key_file, project], stdout=subprocess.PIPE)

        key = "{0}{1}{2}".format(self.server_name, ".", "vhd")
        source_file = "--source-file=gs://{0}/{1}".format(self.bucket_name, key)
        operating_system = "--os={0}".format(self.disk_system)
        quiet = "--quiet"
        result = subprocess.run(
            ["gcloud", "compute", "images", "import",
             self.server_name, source_file, operating_system, quiet], stdout=subprocess.PIPE)

    @audit
    def create_server(self):
        server_capacity = "zones/{0}/machineTypes/{1}".format(
                self.cloud_zone, self.server_capacity)
        disk_name = "projects/{0}/global/images/{1}".format(
                self.project_id, self.server_name)

        body = {
            "name": self.server_name,
            "machineType": server_capacity,
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initializeParams": {
                        "sourceImage": disk_name
                    }
                }
            ],
            "networkInterfaces": [
                {
                    "network": "global/networks/default",
                    "accessConfigs": [
                        {"type": "ONE_TO_ONE_NAT", "name": "External NAT"}
                    ]
                }
            ]
        }

        credentials = service_account.Credentials.from_service_account_info(self.credentials)
        gce = discovery.build("compute", "v1", credentials=credentials)

        gce.instances().insert(
            project=self.project_id,
            zone=self.cloud_zone,
            body=body).execute()

        for _ in range(43200):
            time.sleep(1)
            response = gce.instances().get(
                project=self.project_id,
                zone=self.cloud_zone,
                instance=self.server_name).execute()
            status = response.get("status")
            if status == "RUNNING":
                break


class MicrosoftDriver(BaseDriver):

    def __init__(self, source_or_destination, migration):
        self.server_name = migration.get("virtual_machine")
        migration = migration.get(source_or_destination)
        self.bucket_name = migration.get("container")
        self.server_capacity = migration.get("virtual_machine_size")
        self.network_name = migration.get("network")
        self.subnet_name = migration.get("subnet")
        self.cloud_location = migration.get("location")
        self.cloud_zones = migration.get("zones")
        self.cloud_resource_group_name = migration.get("resource_group_name")
        self.account_name = migration.get("storage_account_name")
        self.account_key = migration.get("storage_account_key")
        client_id = migration.get("client_id")
        client_secret = migration.get("client_secret")
        self.subscription_id = migration.get("subscription_id")
        tenant_id = migration.get("tenant_id")
        active_directory_endpoint_url = migration.get("active_directory_endpoint_url")
        resource_manager_endpoint_url = migration.get("resource_manager_endpoint_url")
        active_directory_graph_resource_id = migration.get("active_directory_graph_resource_id")
        sql_management_endpoint_url = migration.get("sql_management_endpoint_url")
        gallery_endpoint_url = migration.get("gallery_endpoint_url")
        management_endpoint_url = migration.get("management_endpoint_url")
        self.credentials_compute = {}
        self.credentials_compute["clientId"] = client_id
        self.credentials_compute["clientSecret"] = client_secret
        self.credentials_compute["subscriptionId"] = self.subscription_id
        self.credentials_compute["tenantId"] = tenant_id
        self.credentials_compute["activeDirectoryEndpointUrl"] = active_directory_endpoint_url
        self.credentials_compute["resourceManagerEndpointUrl"] = resource_manager_endpoint_url
        self.credentials_compute["activeDirectoryGraphResourceId"] = active_directory_graph_resource_id
        self.credentials_compute["sqlManagementEndpointUrl"] = sql_management_endpoint_url
        self.credentials_compute["galleryEndpointUrl"] = gallery_endpoint_url
        self.credentials_compute["managementEndpointUrl"] = management_endpoint_url

    @audit
    def start_server(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)
        virtual_machine = compute_client.virtual_machines.start(
            resource_group_name=self.cloud_resource_group_name,
            vm_name=self.server_name)
        virtual_machine.wait()

    @audit
    def delete_bucket(self):
        page_blob_service = blob.PageBlobService(
            account_name=self.account_name,
            account_key=self.account_key)
        page_blob_service.delete_container(self.bucket_name)

    @audit
    def delete_server(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)
        virtual_machine = compute_client.virtual_machines.delete(
            resource_group_name=self.cloud_resource_group_name,
            vm_name=self.server_name)
        virtual_machine.wait()

    @audit
    def delete_image(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)
        snapshot = compute_client.snapshots.revoke_access(
            resource_group_name=self.cloud_resource_group_name,
            snapshot_name=self.server_name)
        snapshot.wait()
        snapshot = compute_client.snapshots.delete(
            resource_group_name=self.cloud_resource_group_name,
            snapshot_name=self.server_name)
        snapshot.wait()

    @audit
    def create_bucket(self):
        page_blob_service = blob.PageBlobService(
            account_name=self.account_name,
            account_key=self.account_key)
        page_blob_service.create_container(self.bucket_name)

    @audit
    def stop_server(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)

        virtual_machine = compute_client.virtual_machines.power_off(
            resource_group_name=self.cloud_resource_group_name,
            vm_name=self.server_name)

        virtual_machine.wait()

    @audit
    def export_disk(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)

        virtual_machine = compute_client.virtual_machines.get(
            resource_group_name=self.cloud_resource_group_name,
            vm_name=self.server_name)

        storage_profile = virtual_machine.storage_profile
        operating_system_disk = storage_profile.os_disk
        managed_disk = operating_system_disk.managed_disk
        source_uri = managed_disk.id
        disk_size_gb = operating_system_disk.disk_size_gb

        async_snapshot_creation = compute_client.snapshots.create_or_update(
            resource_group_name=self.cloud_resource_group_name,
            snapshot_name=self.server_name,
            snapshot={
                "location": self.cloud_location,
                "creation_data": {
                    "create_option": "Copy",
                    "source_uri": source_uri,
                    "disk_size_gb": disk_size_gb
                }
            }
        )

        async_snapshot_creation.wait()

    @audit
    def download_disk(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)

        snapshot = compute_client.snapshots.grant_access(
            resource_group_name=self.cloud_resource_group_name,
            snapshot_name=self.server_name,
            access="read",
            duration_in_seconds=18000)
        snapshot.wait()
        access_sas = snapshot.result().access_sas

        blob_name = "{0}.{1}".format(self.server_name, "vhd")
        path = "/home/ubuntu/volume"
        path = os.path.join(path, "{0}".format(blob_name))
        response = requests.get(access_sas, stream=True)

        handle = open(path, "wb")
        for chunk in response.iter_content(chunk_size=512):
            if chunk:
                handle.write(chunk)

        disk = "/home/ubuntu/volume/{0}".format(blob_name)
        print("disk size (bytes): {}".format(os.path.getsize(disk)))

    @audit
    def prepare_disk(self):
        blob_name = "{0}.{1}".format(self.server_name, "vhd")
        file_path = "/home/ubuntu/volume/{0}".format(blob_name)

        temp_name = "{0}.{1}".format(self.server_name, "raw")
        temp_file_path = "/home/ubuntu/volume/{0}".format(temp_name)

        initial = datetime.datetime.now()
        print("convertion from VHD to RAW: started")
        result = subprocess.run(
            ["qemu-img", "convert", "-f", "vpc", "-O", "raw",
             file_path, temp_file_path],
             stdout=subprocess.PIPE)
        final = datetime.datetime.now()
        total = final - initial
        print("{}:{}:{}:total".format("MicrosoftDriver", "from vhd to raw", total.total_seconds()))

        initial = datetime.datetime.now()
        print("determining new VHD size: started")
        command = ["qemu-img", "info", temp_file_path, "--output", "json"]
        result = subprocess.run(command, stdout=subprocess.PIPE)

        result = result.stdout
        info = json.loads(result)
        virtual_size = info.get("virtual-size")
        megabyte = 1024 * 1024
        rounded_size = virtual_size + (megabyte - virtual_size % megabyte)
        rounded_size = str(rounded_size)

        print("virtual_size (byte)")
        print(virtual_size)
        print("rounded_size (byte)")
        print(rounded_size)

        final = datetime.datetime.now()
        total = final - initial
        print("{}:{}:{}:total".format("MicrosoftDriver", "calculate new size", total.total_seconds()))

        initial = datetime.datetime.now()
        print("start resize disk: started")
        command = ["qemu-img", "resize", "-f", "raw", temp_file_path, rounded_size]
        result = subprocess.run(command, stdout=subprocess.PIPE)
        final = datetime.datetime.now()
        total = final - initial
        print("{}:{}:{}:total".format("MicrosoftDriver", "resize disk", total.total_seconds()))

        initial = datetime.datetime.now()
        print("convertion from RAW to VHD: started")
        command = ["qemu-img", "convert", "-f", "raw",
                   "-o", "subformat=fixed,force_size",
                   "-O", "vpc", temp_file_path, file_path]

        result = subprocess.run(command, stdout=subprocess.PIPE)
        final = datetime.datetime.now()
        total = final - initial
        print("{}:{}:{}:total".format("MicrosoftDriver", "from raw to vhd", total.total_seconds()))

    @audit
    def upload_disk(self):

        blob_name = "{0}.{1}".format(self.server_name, "vhd")
        file_path = "/home/ubuntu/volume/{0}".format(blob_name)

        page_blob_service = blob.PageBlobService(
            account_name=self.account_name,
            account_key=self.account_key)

        page_blob_service.create_blob_from_path(
            container_name=self.bucket_name,
            blob_name=blob_name,
            file_path=file_path)

        disk = "/home/ubuntu/volume/{0}".format(blob_name)
        print("disk size (bytes): {}".format(os.path.getsize(disk)))
        os.remove(disk)
        print("disk status: deleted")

    @audit
    def import_disk(self):
        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)

        page_blob_service = blob.PageBlobService(
            account_name=self.account_name,
            account_key=self.account_key)

        blob_name = "{0}.{1}".format(self.server_name, "vhd")
        blob_uri = page_blob_service.make_blob_url(
            container_name=self.bucket_name,
            blob_name=blob_name)

        image = {
            "location": self.cloud_location,
            "storage_profile": {
                "os_disk": {
                    "os_type": "Linux",
                    "os_state": "Generalized",
                    "blob_uri": blob_uri,
                    "caching": "ReadWrite",
                }
            }
        }

        async_image_creation = compute_client.images.create_or_update(
            resource_group_name=self.cloud_resource_group_name,
            image_name=self.server_name,
            parameters=image)
        async_image_creation.wait()

    @audit
    def create_server(self):
        client_class = network.NetworkManagementClient
        network_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)

        subnet = network_client.subnets.get(
            resource_group_name=self.cloud_resource_group_name,
            virtual_network_name=self.network_name,
            subnet_name=self.subnet_name)

        network_interface_name = "{}{}".format(self.server_name, "-interface")
        ip_name = "{}{}".format(self.server_name, "-ip")
        async_nic_creation = network_client.network_interfaces.create_or_update(
            resource_group_name=self.cloud_resource_group_name,
            network_interface_name=network_interface_name,
            parameters={
                "location": self.cloud_location,
                "ip_configurations": [{
                    "name": ip_name,
                    "subnet": {
                        "id": subnet.id
                    }
                }]
            }
        )
        async_nic_creation.wait()

        result = async_nic_creation.result()
        nic_id = result.id

        url_parts = [
            "/subscriptions/", self.subscription_id,
            "/resourceGroups/", self.cloud_resource_group_name,
            "/providers/", "Microsoft.Compute",
            "/images/", self.server_name
        ]

        image_reference_id = "".join(url_parts)

        parameters = {
            "location": self.cloud_location,
            "os_profile": {
                "computer_name": self.server_name,
                "admin_username": "ubuntu",
                "admin_password": "Ubuntu123!"
            },
            "hardware_profile": {
                "vm_size": self.server_capacity
            },
            "storage_profile": {
                "image_reference": {
                    "id": image_reference_id
                }
            },
            "network_profile": {
                "network_interfaces": [{
                    "id": nic_id,
                }]
            }
        }

        if(self.cloud_location != "westus"):
            parameters["zones"] = [self.cloud_zones]

        client_class = compute.ComputeManagementClient
        compute_client = get_client_from_json_dict(
            client_class=client_class,
            config_dict=self.credentials_compute)

        async_vm_creation = compute_client.virtual_machines.create_or_update(
            resource_group_name=self.cloud_resource_group_name,
            vm_name=self.server_name,
            parameters=parameters)
        async_vm_creation.wait()


class KumoConductor:

    def __init__(self, migration):
        self.migration = json.loads(migration)
        self.source = self.create_driver("source_account")
        self.destination = self.create_driver("destination_account")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def create_driver(self, source_or_destination):
        account = self.migration.get(source_or_destination)
        account_name = account.get("cloud")
        if (account_name == "amazon"):
            return AmazonDriver(source_or_destination, self.migration)
        if (account_name == "google"):
            return GoogleDriver(source_or_destination, self.migration)
        if (account_name == "microsoft"):
            return MicrosoftDriver(source_or_destination, self.migration)


@task
def migrate(migration):

    with KumoConductor(migration) as conductor:
        # conductor.destination.delete_server()
        # conductor.destination.delete_image()
        # conductor.destination.delete_bucket()
        # conductor.source.delete_bucket()
        # conductor.source.delete_image()
        # conductor.source.start_server()
        conductor.source.create_bucket()
        conductor.destination.create_bucket()
        conductor.source.stop_server()
        conductor.source.export_disk()
        conductor.source.download_disk()
        conductor.destination.prepare_disk()
        conductor.destination.upload_disk()
        conductor.destination.import_disk()
        conductor.destination.create_server()
