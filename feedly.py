import csv
import logging

import requests
from requests_html import HTMLSession
from dagster_cron import SystemCronScheduler
from dagster import (
    RepositoryDefinition,
    ScheduleDefinition,
    pipeline,
    schedules,
    solid,
)

from env import FEEDLY_REFRESH_TOKEN
from firebase import firebase_ops
from entry import Entry

class EtlFeedlyFirebase:
    def __init__(self, feedly_refresh_token=FEEDLY_REFRESH_TOKEN, context=None):
        self.access_token = self._get_access_token(feedly_refresh_token)
        self.user_id = self._get_user_id()
        if context:
            self.logger = context.log
        else:
            self.logger = logging.getLogger()

    def execute(self):
        self._etl_feedly_to_firebase()

    def _get_access_token(self, feedly_refresh_token):
        url = "https://cloud.feedly.com/v3/auth/token"
        payload = dict(
            refresh_token=feedly_refresh_token,
            client_id="feedlydev", 
            client_secret="feedlydev",
            grant_type="refresh_token"
        )
        resp = requests.post(url, json=payload).json()
        access_token = resp['access_token']

        return access_token

    def _get_user_id(self):
        url = "https://cloud.feedly.com/v3/profile"
        headers = {'Authorization': f"OAuth {self.access_token}"}

        resp = requests.get(url, headers=headers).json()
        user_id = resp['id']

        return user_id

    def _fetch_entries(self, continuation=None):
        url = f"https://cloud.feedly.com/v3/streams/contents?streamId=user/{self.user_id}/tag/global.saved"
        headers = {'Authorization': f"OAuth {self.access_token}"}
        
        params = dict(count=500, ranked="newest")
        if continuation:
            params['continuation'] = continuation
        
        resp = requests.get(url, headers=headers, params=params).json()
        continuation = resp.get('continuation')
        entries = resp['items']
        
        return entries, continuation

    def _get_site_contents(self, url, entry_id):
        dest_raw_html = ""
        og_image = ""
        
        s = HTMLSession()
        try:
            resp = s.get(url)
            if resp.status_code == 200:
                dest_raw_html = f'{entry_id}/raw_html.txt'
                firebase_ops.upload_from_string(blob_id=dest_raw_html, string=resp.html.raw_html)
                og_ele = resp.html.find('head > meta[property="og:image"]', first=True)
                if og_ele:
                    og_image = og_ele.attrs['content']
        except:
            self.logger.info(f"failed to access {url}")
            
        return dest_raw_html, og_image

    def _etl_feedly_to_firebase(self):
        continuation = None
        while True:
            feedly_entries, continuation = self._fetch_entries(continuation=continuation)
            for feedly_entry in feedly_entries:
                entry = Entry(
                id=feedly_entry['originId'].replace("/", "_"),
                title=feedly_entry['title'],
                published=feedly_entry['published'],
                url=feedly_entry['alternate'][0]['href'],
                feedly_id=feedly_entry['id']
                )
                if 'summary' in feedly_entry:
                    entry.summary = feedly_entry['summary'].get('content')

                if "commonTopics" in feedly_entry:
                    entry.commonTopics = feedly_entry['commonTopics']

                if "origin" in feedly_entry:
                    entry.src = feedly_entry['origin']['streamId']

                entry.raw_html, entry.og_image = self._get_site_contents(entry.url, entry.id)

                if not firebase_ops.entry_exists(entry.id):
                    self.logger.info(f"added {entry.id}")
                    firebase_ops.set_entry(entry.id, entry.to_dict())
                else:
                    self.logger.info(f"entry_id: {entry.id} already exists")
                    continuation = None
                    break
            if continuation is None:
                self.logger.info("finished")
                break



@solid
def load_to_firebase(context):
    EtlFeedlyFirebase(context=context).execute()


@pipeline
def load_to_firebase_pipeline():
    load_to_firebase()


def etl_feedly_repository():
    return RepositoryDefinition(
        'etl_feedly_repository', pipeline_defs=[load_to_firebase_pipeline]
    )


@schedules(SystemCronScheduler)
def feedly_schedules():
    return [
        ScheduleDefinition(
            name='daily_batch',
            cron_schedule='0 15 * * *',
            pipeline_name='load_to_firebase_pipeline',
            environment_dict={},
        )
    ]

if __name__ == '__main__':
    # EtlFeedlyFirebase().execute()
    result = execute_pipeline(hello_pipeline)
    assert result.success


# def test_hello_cereal_solid():
#     res = execute_solid(hello_cereal)
#     assert res.success
#     assert len(res.output_value()) == 77


# def test_hello_cereal_pipeline():
#     res = execute_pipeline(hello_cereal_pipeline)
#     assert res.success
#     assert len(res.result_for_solid('hello_cereal').output_value()) == 77