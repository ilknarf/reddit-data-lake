import sys
import os

# import re
from datetime import datetime

import praw
from textblob import TextBlob

import boto3
import json

import logging

firehose_client = boto3.client('firehose', region_name='us-east-2')
firehose_stream_name = os.environ['FIREHOSE_STREAM_NAME']

def push_to_firehose(data):
    try:
        res = firehose_client.put_record(
            DeliveryStreamName=firehose_stream_name,
            Record={'Data': (json.dumps(data, ensure_ascii=False) + '\n').encode('utf8')}
        )

        logging.info(res)
    except:
        logging.exception('error pushing to firehose')

# https://gist.github.com/slowkow/7a7f61f495e3dbb7e3d767f97bd7304b
# def remove_emoji(string):
#     emoji_pattern = re.compile("["
#                            u"\U0001F600-\U0001F64F"  # emoticons
#                            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#                            u"\U0001F680-\U0001F6FF"  # transport & map symbols
#                            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                            u"\U00002702-\U000027B0"
#                            u"\U000024C2-\U0001F251"
#                            "]+", flags=re.UNICODE)
#     return emoji_pattern.sub(r'', string)

# sentiment analysis
def get_sentiment(comment):
    pat_analysis = TextBlob(comment)
    return pat_analysis.sentiment

if __name__ == '__main__':  
    # get PRAW config
    client_id = os.environ['PRAW_CLIENT_ID']
    client_secret = os.environ['PRAW_CLIENT_SECRET']
    user_agent = os.environ['PRAW_USER_AGENT']

    # get subreddits
    subreddits = '+'.join(sys.argv[1:])

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    # use set to track keys of already-processed submissions
    processed = set()
    count = 0

    logging.info('Connected to Reddit!')

    while True:
        # lazy comment stream
        controversial = reddit.subreddit(subreddits).comments()
        new_processed = set()

        for comment in controversial:
            if comment.id not in processed:
                # strip emojis and other unicode chars
                # cleaned = remove_emoji(str(comment.body))
                cleaned = str(comment.body)

                # get date
                date = datetime.utcfromtimestamp(comment.created_utc)
                date_iso = date.strftime('%Y-%m-%d %H:%M:%S')

                # get sentiment analysis
                sentiment = get_sentiment(cleaned)
                subjectivity = sentiment.subjectivity
                polarity = sentiment.polarity

                comment_json = {
                    '@timestamp': date_iso,
                    'id': comment.id,
                    'subreddit': comment.subreddit,
                    'body': cleaned,
                    'is_submitter': comment.is_submitter,
                    'polarity': polarity,
                    'subjectivity': subjectivity,
                    'author': comment.author.name,
                }
                
                new_processed.add(comment.id)
                logging.info(f'Processed comment {count} id={comment.id} polarity={polarity} subjectivity={subjectivity}')
                push_to_firehose(comment_json)

                # increment overall count
                count += 1

        processed = new_processed
