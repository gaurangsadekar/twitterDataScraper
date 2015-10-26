from tweepy.streaming import StreamListener
import json
from pymongo.mongo_client import MongoClient

# Some user defined keywords for filtering
keywords = ["love","football","tech","trump","india"]

class StreamTweetListener(StreamListener):
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client.tweetDB
        self.tweets = self.init_collection('tweetRecords')

    def init_collection(self, collection_name):
        if collection_name not in self.db.collection_names():
            collection = self.db.create_collection(collection_name)
        else:
            collection = self.db[collection_name]
        return collection

    def on_data(self, tweet):
        json_tweet = json.loads(tweet)
        if "coordinates" in json_tweet:
            coordinates =  json_tweet['coordinates']
            if coordinates is not None:
                # Finding and Storing keywords
                # for keyword in keywords:
                #     if keyword in json_tweet['text'].lower():
                #         json_tweet["keyword"] = keyword;
                # Store it in MongoDB
                print "Inserting into DB"
                parsed_tweet = {}
                parsed_tweet["tweet_id"] = json_tweet['id']
                parsed_tweet["tweet_id_str"] = json_tweet['id_str']
                parsed_tweet["tweet_created_at"] = json_tweet['created_at']
                parsed_tweet["tweet_timestamp"] = json_tweet['timestamp_ms']
                parsed_tweet["tweet_lang"] = json_tweet['lang']
                parsed_tweet["tweet_text"] = json_tweet['text']
                parsed_tweet["geo"] = json_tweet['geo']
                parsed_tweet["retweet_count"] = json_tweet['retweet_count']
                parsed_tweet["favorite_count"] = json_tweet['favorite_count']
                parsed_tweet["coordinate"] = json_tweet['coordinates']
                
                dump_tweet = json.dumps(parsed_tweet)
                loaded_tweet = json.loads(dump_tweet)
                self.tweets.insert_one(loaded_tweet)
        return True

    def on_error(self, status):
        print "Error: "+ status + "\n"
        return False

    def on_status(self, status):
        print "Status: ", status
