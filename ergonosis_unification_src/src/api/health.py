from flask import Flask, jsonify


def create_health_app(storage_factory):
    app = Flask(__name__)

    @app.route("/health")
    def health():
        try:
            storage = storage_factory()
            healthy = storage.health_check()
        except Exception:
            healthy = False
        status = "ok" if healthy else "degraded"
        return jsonify({"status": status}), (200 if healthy else 503)

    @app.route("/ready")
    def ready():
        return health()

    return app
