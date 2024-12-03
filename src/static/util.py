from pathlib import Path

import dotenv
import sqlalchemy
from crewai.telemetry import Telemetry

dotenv.load_dotenv()

DB_PASSWORD="GDSC2QKa24EE"
DB_USER="gdsc_participant"
DB_ENDPOINT="unesco-reader.crqaeg62obh7.us-east-1.rds.amazonaws.com"
DB_PORT=5432

__db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_ENDPOINT}:{DB_PORT}/postgres'
ENGINE = sqlalchemy.create_engine(__db_url)

PROJECT_ROOT = Path(__file__).parent.parent


# Disable CrewAI Telemetry
def noop(*args, **kwargs):
    pass


for attr in dir(Telemetry):
    if callable(getattr(Telemetry, attr)) and not attr.startswith("__"):
        setattr(Telemetry, attr, noop)
