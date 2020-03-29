from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv(Path.home()/".env")

FEEDLY_REFRESH_TOKEN = os.environ['FEEDLY_REFRESH_TOKEN']
ELASTIC_USERNAME = os.environ['ELASTIC_USERNAME']
ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']
ELASTIC_BASE_URL = os.environ['ELASTIC_BASE_URL']