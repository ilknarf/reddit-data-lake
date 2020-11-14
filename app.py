#!/usr/bin/env python3

from aws_cdk import core

from stack import RedditDataLakeStack


app = core.App()
RedditDataLakeStack(app, "reddit-data-lake")

app.synth()
