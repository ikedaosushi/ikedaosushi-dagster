import os
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
from dagster_cron import SystemCronScheduler
from dagster import (
    RepositoryDefinition,
    ScheduleDefinition,
    pipeline,
    schedules,
    solid,
)

from env import FEEDLY_REFRESH_TOKEN, SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET

credentials = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
spotify = spotipy.Spotify(client_credentials_manager=credentials)

class MusicFeeder:
    scopes = " ".join([ 
        "ugc-image-upload",
        "user-read-playback-state",
        "user-modify-playback-state",
        "user-read-currently-playing",
        "streaming",
        "app-remote-control",
        "user-read-email",
        "user-read-private",
        "playlist-read-collaborative",
        "playlist-modify-public",
        "playlist-read-private",
        "playlist-modify-private",
        "user-library-modify",
        "user-library-read",
        "user-top-read",
        "user-read-recently-played",
        "user-follow-read",
        "user-follow-modify"
    ])
    feedly_username = '5066bdab-2d04-4094-b7ac-cd19047daffe'
    spotify_username = "21bpmv7tcs4av5sakwfxkof2a"
    spotify_playlist_id = "6qsodHkxMB36VhWfIwvylQ"
    stop_words = ["MV"]
    
    def __init__(self):
        # feedly setup
        self.feedly_access_token = self._get_feedly_access_token()
        # spotify setup
        self.token = util.prompt_for_user_token(self.spotify_username, self.scopes)
        self.spotify = spotipy.Spotify(auth=self.token)
        self.titles, self.tracks, self.current_tracks, self.tracks_to_add = None, None, None, None
        
    def execute(self):
        self.titles = self._get_titles()
        # extract latest 30 titles
        self.titles = self.titles[:30]
        self.tracks = self._get_taget_tracks(self.titles)
        self.current_tracks = self._get_current_tracks()
        self.tracks_to_add = self._get_tracks_to_add(self.tracks, self.current_tracks)
        self._add_to_spotify(self.tracks_to_add)
        
    def _get_feedly_access_token(self):        
        url = "https://cloud.feedly.com/v3/auth/token"
        payload = dict(
            refresh_token=FEEDLY_REFRESH_TOKEN,
            client_id="feedlydev", 
            client_secret="feedlydev",
            grant_type="refresh_token"
        )
        resp = requests.post(url, json=payload).json()
        return resp['access_token']
        
    def _request_to_feedly(self, url, params={}):
        headers = {'Authorization': f"OAuth {self.feedly_access_token}"}
        return requests.get(url, headers=headers, params=params).json()
        
    def _get_titles(self):
        url = f"https://cloud.feedly.com/v3/streams/contents?streamId=user/{self.feedly_username}/category/Music"
        params = dict(unreadOnly=False, count=100)
        resp = self._request_to_feedly(url, params)
        return [item['title'] for item in resp['items'] if item['title']]

    def _get_taget_tracks(self, titles):
        one_week_ago = datetime.today() - timedelta(days=7)
        tracks = []
        for title in titles:
#             print("trying...", title)
            terms = re.findall("[a-zA-Z ]+", title)
            terms = [term.strip() for term in terms if term.strip()]
            terms = [term for term in terms if term not in self.stop_words]
            query = " ".join(terms)
            logging.info(f"searching: {query}")
            result = self.spotify.search(query)['tracks']['items']
            if result:
                track = result[0]
                try:
                    release_date = datetime.strptime(track['album']['release_date'], "%Y-%m-%d")
                except ValueError:
                    continue

                # Don't append if the release date of the track is older than one week ago
                if release_date < one_week_ago:
                    continue
                tracks.append(track['id'])
                
        return tracks
    
    def _get_current_tracks(self):
        return self.spotify.user_playlist(self.spotify_username, self.spotify_playlist_id)['tracks']['items']
    
    def _get_tracks_to_add(self, tracks, current_tracks):
        current_track_ids = [c_track['track']['id'] for c_track in current_tracks]
        tracks = [track for track in tracks if track not in current_track_ids]
        return tracks
    
    def _add_to_spotify(self, tracks):
        if not tracks:
            return 
        self.spotify.user_playlist_add_tracks(self.spotify_username, self.spotify_playlist_id, tracks, position=0)

@solid
def load_to_spotify(context):
    MusicFeeder().execute()


@pipeline
def load_to_spotify_pipeline():
    load_to_spotify()


def etl_spotify_repository():
    return RepositoryDefinition(
        'etl_spotify_repository', pipeline_defs=[load_to_spotify_pipeline]
    )


@schedules(SystemCronScheduler)
def spotify_schedules():
    return [
        ScheduleDefinition(
            name='daily_spotify_batch',
            cron_schedule='0 * * * *',
            pipeline_name='load_to_spotify_pipeline',
            environment_dict={},
        )
    ]

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    MusicFeeder().execute()
    # EtlFeedlyspotify(context=context).execute()
    # result = execute_pipeline(load_to_spotify_pipeline)
    # assert result.success


# def test_hello_cereal_solid():
#     res = execute_solid(hello_cereal)
#     assert res.success
#     assert len(res.output_value()) == 77


# def test_hello_cereal_pipeline():
#     res = execute_pipeline(hello_cereal_pipeline)
#     assert res.success
#     assert len(res.result_for_solid('hello_cereal').output_value()) == 77
