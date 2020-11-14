import sys
import os

from collections import deque
import re

import praw
from textblob import Blobber, TextBlob

# https://gist.github.com/slowkow/7a7f61f495e3dbb7e3d767f97bd7304b
def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

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

    print('========================================')
    print('Connected to Reddit!')

    while True:
        # lazy comment stream
        controversial = reddit.subreddit(subreddits).comments(limit=100).controversial()
        new_processed = set()

        for comment in controversial:
            if comment.id not in processed:
                cleaned = remove_emoji(str(comment.body))
                sentiment = get_sentiment(cleaned)
                
                new_processed.add(comment.id)

        processed = new_processed
