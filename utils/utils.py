import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy.engine import URL

load_dotenv(find_dotenv(), override=False)

def build_db_url() -> URL:
    return URL.create(
        "postgresql+psycopg",
        username=os.getenv("DB_USER", "kekoa"),
        password=os.getenv("DB_PASSWORD", " "),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),
        database=os.getenv("DB_NAME", "dodgers_pitching"),
    )
