import flask

from kumo.conductor import conductor


app = flask.Flask(__name__)


@app.route("/migrate", methods=["POST"])
def migrate():
    """
    Create a migration.
    ---
    parameters:
      - name: json
        in: body
        required: true
        description: Create migration.
        schema:
          id: Migration
          required:
            - json
          properties:
            json:
              type: string
              description: Migration json.
    responses:
      201:
        description: Created
    """
    conductor.migrate.delay(flask.request.json)
    return (flask.jsonify(), 201)
