# Twitter Stream to Kinesis

This app provides the ability of query in Twitter stream tweets, and implemented in Python.
The purpose is to listen to Twitter api stream, filter tweets that are matched to search expression and write those tweets to a file.

## Application Description

There are two parts that have been implemented:

* twitter_producer
* tweets_consumer

## Preparations

In order to run this, there are a few things that need to be in place:

* To get started, you will need credentials from Twitter to make calls to its public API. For this, go to [Twitter api](apps.twitter.com) and click create a new app and fill out the form to get your unique credentials for making requests from Twitter for their data.  
  Once you have your Twitter credentials you can put them in the config file
* Create a stream at aws Kinesis
* Run ```bash
pip install -r requirements.txt```

### twitter_producer

The `twitter_producer` application is the part that gets the stream of tweets from
Twitter. It uses the [Twitter Streaming API](https://dev.twitter.com/streaming/overview), and post tweets in bulk to an Amazon Kinesis stream.
Connection to aws Kinesis using [boto3](https://boto3.readthedocs.io/en/latest/) library  
Connection to Twitter using [TwitterAPI](https://github.com/geduldig/TwitterAPI) library

### tweets_consumer

Script parameters:  
1. search_expression - string

The `tweets_consumer` application is the part that pull the stream of tweets from
Kinesis. It uses the [boto3](https://boto3.readthedocs.io/en/latest/) to get tweets from stream.
After get records from stream, tweet is parsed and we are checking for match.
Once we find a match between tweet and search_expression - we write tweet to csv file.



## Config file

Configuration file that contain the following variables:
1. consumer_key
2. consumer_secret
3. access_token_key
4. access_token_secret
5. partition_key
6. stream_name
7. tweets_location
8. bulk_size
9. output_file
10. shard_id
11. shard_iterator_type

## Running

Start the modules in order, ie first `twitter_producer.py`, then `tweets_consumer.py` with search_expression as an argument 

## Output

Tweets that contain search_expression will be filled into a csv file
