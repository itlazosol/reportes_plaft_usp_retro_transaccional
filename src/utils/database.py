from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.utils import config_app

engines  = {
    'pg': create_engine(config_app.Config.PG_URI)
}

SessionLocal = sessionmaker(bind=engines['pg'])
