import os
import sys

from flask import Flask
from flask_cors import CORS

# Add project root to sys.path so lottery_scraper is importable
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from lottery_scraper.db import init_db
from lottery_scraper.config import DB_PATH as DEFAULT_DB_PATH


def create_app():
    app = Flask(__name__)

    db_path = os.environ.get("LOTTERY_DB", DEFAULT_DB_PATH)
    app.config["LOTTERY_DB"] = db_path

    CORS(app, origins=[
        "http://localhost:*",
        "http://127.0.0.1:*",
        "https://*.github.io",
    ])

    init_db(db_path)

    from api.routes.draws import draws_bp
    from api.routes.check import check_bp
    app.register_blueprint(draws_bp)
    app.register_blueprint(check_bp)

    return app


app = create_app()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
