from pathlib import Path
import os

from dotenv import load_dotenv
load_dotenv(Path.home()/".env")

FEEDLY_REFRESH_TOKEN = os.environ['FEEDLY_REFRESH_TOKEN']
SPOTIPY_CLIENT_ID = os.environ['SPOTIPY_CLIENT_ID']
SPOTIPY_CLIENT_SECRET = os.environ['SPOTIPY_CLIENT_SECRET']
SPOTIPY_REDIRECT_URI = os.environ['SPOTIPY_REDIRECT_URI']