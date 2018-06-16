import csv
import json
import logging
import sys
import time
import boto3
import config


def main():
    # Setup basic logging format
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    search_expression = None
    try:
        search_expression = sys.argv[1]
    except:
        logging.error("please provide script parameter with the value to filter")
        sys.exit(1)

    # connect to kinesis aws service using boto3
    kinesis = boto3.client("kinesis")

    # get connection to the kinesis stream that was created previously
    pre_shard_it = kinesis.get_shard_iterator(
        StreamName=config.stream_name, ShardId=config.shard_id, ShardIteratorType=config.shard_iterator_type)
    shard_it = pre_shard_it["ShardIterator"]

    write_tweet_to_csv_file(["Tweet Text", "Tweet Username", "Tweet URL", "Tweet Location"])

    # consume records from stream, record by record
    while True:
        data = kinesis.get_records(ShardIterator=shard_it, Limit=1)
        shard_it = data["NextShardIterator"]

        tweets = process_records(data["Records"])
        for tweet in tweets:
            if filter_expression(search_expression, tweet):
                logging.info("found match ! write tweet to file")
                tweet = [tweet["text"].replace("\n", " "), tweet["username"], tweet["url"], tweet["location"]]
                write_tweet_to_csv_file(tweet)

        time.sleep(1.0)


def process_records(records):
    tweets = []
    try:
        # go over each record and parse tweet data
        for record in records:
            # define data section in single record
            tweet_data = json.loads(record["Data"])
            try:
                # get tweet text
                if "full_text" in tweet_data:
                    text = (tweet_data["full_text"]).encode("utf-8")
                elif "text" in tweet_data:
                    text = (tweet_data["text"]).encode("utf-8")
                else:
                    text = "N/A"
                # get tweet user
                if "name" in tweet_data:
                    username = (tweet_data["name"]).encode("utf-8")
                elif "user" in tweet_data:
                    username = (tweet_data["user"]["name"]).encode("utf-8")
                else:
                    username = "N/A"
                # get tweet url
                if "quoted_status_permalink" in tweet_data:
                    url = (tweet_data["quoted_status_permalink"]["url"]).encode("utf-8")
                elif "urls" in tweet_data:
                    url = (tweet_data["urls"]["url"]).encode("utf-8")
                else:
                    url = "N/A"
                # get tweet location
                location = (tweet_data["place"]["full_name"]).encode("utf-8") if "place" in tweet_data else None

                logging.debug("going over the following tweet: ")
                logging.debug("1. text= " + text)
                logging.debug("2. username= " + str(username))
                logging.debug("3. url= " + str(url))
                logging.debug("4. location= " + str(location))
                tweet = {
                    "text": text,
                    "username": username,
                    "url": url,
                    "location": location
                }
                tweets.append(tweet)
            except Exception as e:
                logging.error("failed to parse tweet... error info: " + str(e))
    except Exception as e:
        logging.error("failed to parse record... error info: " + str(e))
    return tweets


def write_tweet_to_csv_file(row):
    try:
        with open(config.output_file, "a") as csv_file:
            writer = csv.writer(csv_file, delimiter=',', lineterminator="\n")
            writer.writerow(row)
            csv_file.flush()
    except Exception as e:
        logging.error("failed to write tweet to file :( info: " + str(e))


def filter_expression(search_expression, tweet):
    return (search_expression in tweet["text"]) or (search_expression in tweet["username"]) or (
        search_expression in tweet["url"]) or (
               search_expression in tweet["location"])


if __name__ == '__main__':
    main()
