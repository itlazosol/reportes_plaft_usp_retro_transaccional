from decouple import config
from urllib.parse import quote_plus

class Config:
    PG_USER = config('PG_USER')
    PG_PASSWORD = config('PG_PASSWORD')
    PG_HOST = config('PG_HOST')
    PG_PORT = config('PG_PORT')
    PG_DATABASE = config('PG_DATABASE')
    
    # PostgreSQL principal
    PG_URI = f"postgresql://{PG_USER}:{quote_plus(PG_PASSWORD)}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
