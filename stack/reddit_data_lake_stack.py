from aws_cdk import (
    core,
    aws_kinesisfirehose as kf,
    aws_iam as iam,
    aws_glue as glue,
)

BUCKET_ARN = 'arn:aws:s3:::reddit-data-lake-target'

class RedditDataLakeStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        glue_db = glue.Database(
            self, 'GlueDB', database_name='reddit_data',
        )

        # input data schema
        glue_table = glue.Table(
            self, 'GlueTable',
            table_name='sentiment',
            columns=[
                glue.Column(name='@timestamp', type=glue.Schema.TIMESTAMP),
                glue.Column(name='id', type=glue.Schema.INTEGER),
                glue.Column(name='subreddit', type=glue.Schema.STRING),
                glue.Column(name='body', type=glue.Schema.STRING),
                glue.Column(name='is_submitter', type=glue.Schema.BOOLEAN),
                glue.Column(name='polarity', type=glue.Schema.FLOAT),
                glue.Column(name='subjectivity', type=glue.Schema.FLOAT),
                glue.Column(name='author', type=glue.Schema.STRING),
            ],
            database=glue_db,
            data_format=glue.DataFormat.JSON,
        )

        stream_role = iam.Role(
            self, 'FirehoseRole',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com'),
            description='role used by Firehose to access s3 bucket',
        )

        # add s3 statement
        stream_role.add_to_policy(
            iam.PolicyStatement(
                resources=[BUCKET_ARN, f'{BUCKET_ARN}/*'],
                actions=[
                    's3:AbortMultipartUpload',
                    's3:GetBucketLocation',
                    's3:GetObject',
                    's3:ListBucket',
                    's3:ListBucketMultipartUploads',
                    's3:PutObject',
                ],
            )
        )

        # add glue statement
        stream_role.add_to_policy(
            iam.PolicyStatement(
                resources=[glue_table.table_arn],
                actions=[
                    'glue:GetTable',
                    'glue:GetTableVersion',
                    'glue:GetTableVersions',
                ],
            )
        )

        s3_config = kf.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn=BUCKET_ARN,  # temporary, will replace with env variable
            role_arn=stream_role.role_arn,
            prefix='reddit/',
        )

        firehose = kf.CfnDeliveryStream(
            self, 'FirehoseStream',
            delivery_stream_name='RedditDataStream',
            s3_destination_configuration=s3_config,
        )
