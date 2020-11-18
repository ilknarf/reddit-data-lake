#!/usr/bin/env python3

from aws_cdk import core

from stack.reddit_data_lake_stack import RedditDataLakeStack

env = core.Environment(
    region='us-east-2',
)

app = core.App()
RedditDataLakeStack(app, 'reddit-data-lake', env=env)

app.synth()
