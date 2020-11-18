# Streaming Reddit Data to S3

This projects analyzes real-time Reddit data, performs sentiment analysis,
and stores to S3 buckets using Kinesis Firehose. Uses AWS CDK for
infrastructure-as-code.

Progress:
- [x] Reddit API Sentiment Analysis Application (to be deployed on ECS Fargate).
- [x] Hook up to Firehose
- [ ] Automate deployment using AWS CDK

In order to build, make sure that the AWS CDK CLI is installed using
`npm install -g aws-cdk@latest`

Next, make sure that your Reddit credentials are exported
```
export PRAW_CLIENT_SECRET=[YOUR API KEY SECRET]
export PRAW_CLIENT_ID=[YOUR API KEY ID]
export PRAW_USER_AGENT=[YOUR USER AGENT]
```

From the project root
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `cdk deploy`
