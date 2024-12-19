# include/config.py
import os
from dotenv import load_dotenv


load_dotenv()

# Datalake Configuration
DATALAKE_TYPE = os.getenv("DATALAKE_TYPE", "local")

# S3 Config
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

# Breweries API Config
BREWERY_API_URL = 'https://api.openbrewerydb.org/v1/breweries'
