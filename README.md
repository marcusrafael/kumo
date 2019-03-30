![kumo](https://github.com/marcusrafael/kumo/blob/master/images/kumo.png)
<h2 align="center">Kumo Service</h2>

## Instalation

```bash
sudo apt update
```
```bash
sudo apt install -y docker.io
```
```bash
sudo usermod -aG docker ubuntu
```
```bash
newgrp docker
```
```bash
git clone https://github.com/marcusrafael/kumo
```
```bash
cd kumo
```
```bash
docker build -t kumo .
```
```bash
docker run -d -p 5000:5000 -v kumo-volume:/home/ubuntu/volume --name kumo kumo
```
```bash
docker logs -f kumo
```

## Extra commands

```bash
git config --global user.name ""
```
```bash
git config --global user.email ""
```
```bash
docker container stop kumo; docker container rm kumo; docker volume rm kumo-volume
```
```bash
dd if=/dev/urandom of=kumo.txt bs=300000000 count=100 iflag=fullblock
```
