#!/usr/bin/env python3

from aws_cdk import core

from stack.reddit_data_lake_stack import RedditDataLakeStack


app = core.App()
RedditDataLakeStack(app, "reddit-data-lake")

app.synth()
