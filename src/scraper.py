from typing import List, Any, Dict
import tweepy
import pandas as pd
import datetime
import json
import boto3


class Scraper:
    def __init__(self, twitter_account: Any, client: Any):
        self.client = client
        self.twitter_account = twitter_account
    
    @staticmethod
    def _write_json(d: Dict, file: str):
        with open(file, "w") as fi:
            fi.write(json.dumps(d, indent=4))
    
    @staticmethod
    def _read_json(file: str)-> Dict:
        with open(file, "r") as fi:
            d = json.loads(fi.read())
        return d

    @staticmethod
    def _from_ids_to_name(client: Any, l_account: List)-> Dict:
        d = {}
        if (len(l_account) >= 100):
            l_account = [l_account[i:i +100] for i in range(0, len(l_account), 100)]
            for unames in l_account:
                users = client.get_users(usernames=unames)
                ids = [x.id for x in users.data]
                for id, name in zip(ids, unames):
                    d[name] = id
        else:
            users = client.get_users(usernames=l_account)
            ids = [x.id for x in users.data]
            for id, name in zip(ids, l_account):
                d[name] = id
        return d

    @staticmethod
    def _get_tweets_from_ids(client: Any, l_account: List, start_date: datetime.datetime, end_date: datetime.datetime):
        raw_data = []
        for x in l_account:
            request = client.get_users_tweets(
                id=x,
                max_results=100,
                tweet_fields=["created_at","author_id","public_metrics","referenced_tweets","entities"],
                start_time=start_date.isoformat("T")+"Z",
                end_time=end_date.isoformat("T")+"Z"
            )
            if request.data != None:
                raw_data.append(request.data)
        raw_data = [x for l in raw_data for x in l]
        return raw_data

    @staticmethod
    def _transform_data(data: List)-> pd.DataFrame:
        df = pd.DataFrame()
        if len(data) > 0:
            l_created = []
            l_id_tweet = []
            l_id_author = []
            l_text = []
            l_count_rt = []
            l_count_rep = []
            l_count_like = []
            l_type = [] #case retweet or normal tweet
            l_entities = []
            for x in data:
                l_created.append(x["created_at"])
                l_id_tweet.append(x["id"])
                l_id_author.append(x["author_id"])
                l_text.append(x["text"])
                l_count_rt.append(x["public_metrics"]["retweet_count"])
                l_count_rep.append(x["public_metrics"]["reply_count"])
                l_count_like.append(x["public_metrics"]["like_count"])
                if x["referenced_tweets"] != None:
                    l_type.append(x["referenced_tweets"][0]["type"])
                else:
                    l_type.append("tweet")
                l_entities.append(x["entities"])
            df = pd.DataFrame(data={
                "created_at": l_created,
                "id_tweet": l_id_tweet,
                "id_author": l_id_author,
                "text": l_text,
                "count_rt": l_count_rt,
                "count_rep": l_count_rep,
                "count_like": l_count_like,
                "type": l_type,
                "entities": l_entities
            })
        else:
            print("empty raw_data")
        return df


    def extract_ids_name(self):
        l_account = self.twitter_account
        client = self.client
        d = self._from_ids_to_name(
            client=client,
            l_account=l_account
        )
        self._write_json(d=d, file="data/name_ids.json")
        return 0

    def get_tweets(self, start_date: datetime.datetime, end_date: datetime.datetime)-> List:
        account_scraped = list(self._read_json("data/name_ids.json").values())
        account_scraped = account_scraped[:1400]
        raw_data = self._get_tweets_from_ids(
            client=self.client,
            l_account=account_scraped,
            start_date=start_date,
            end_date=end_date
        )
        return raw_data

    def transform_data(self, raw_data: List)-> pd.DataFrame:
        df = pd.DataFrame()
        if all(isinstance(x, tweepy.tweet.Tweet) for x in raw_data):
            df = self._transform_data(data=raw_data)
        else:
            print("Transform_data function : Wrong type into raw_data, not equal to tweepy.tweet.Tweet")
        return df

    def export_data(self, key_id: str, access_key: str, file_name: str):
        client_s3 = boto3.client('s3',
            aws_access_key_id=key_id,
            aws_secret_access_key=access_key
        )
        client_s3.upload_file(Filename=file_name,
            Bucket='mathieu-data',
            Key=file_name
        )