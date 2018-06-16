from TwitterAPI import TwitterAPI
import json
import config
import boto3
import logging

# Setup basic logging format
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

# connection to aws kinesis using boto3
kinesis = boto3.client('kinesis')

## twitter credentials
consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_token_key = config.access_token_key
access_token_secret = config.access_token_secret

# connection to twitter stream through twitter api
api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
twitter_stream = api.request('statuses/filter', {'locations': config.tweets_location})

# push tweets to kinesis stream in bulk
tweets = []
count = 0
logging.info("starting to stream data from twitter into kinesis")
for item in twitter_stream:
    jsonItem = json.dumps(item)
    tweets.append({'Data': jsonItem, 'PartitionKey': config.partition_key})
    count += 1
    if count == config.bulk_size:
        logging.info("pushing bulk of " + str(config.bulk_size) + " tweets into kinesis")
        kinesis.put_records(StreamName=config.stream_name, Records=tweets)
        count = 0
        tweets = []
