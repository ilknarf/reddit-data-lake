import os

from aws_cdk import (
    core,
    aws_kinesisfirehose as kf,
    aws_iam as iam,
    aws_glue as glue,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_s3 as s3,
)

BUCKET_ARN = os.environ['S3_BUCKET_ARN']

class RedditDataLakeStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # create db for glue schema
        glue_db = glue.Database(
            self, 'GlueDB', database_name='reddit_data',
        )

        # data schema
        glue_table = glue.Table(
            self, 'GlueTable',
            table_name='sentiment',
            columns=[
                glue.Column(name='@timestamp', type=glue.Schema.TIMESTAMP),
                glue.Column(name='id', type=glue.Schema.STRING),
                glue.Column(name='subreddit', type=glue.Schema.STRING),
                glue.Column(name='body', type=glue.Schema.STRING),
                glue.Column(name='is_submitter', type=glue.Schema.BOOLEAN),
                glue.Column(name='polarity', type=glue.Schema.FLOAT),
                glue.Column(name='subjectivity', type=glue.Schema.FLOAT),
                glue.Column(name='author', type=glue.Schema.STRING),
            ],
            database=glue_db,
            data_format=glue.DataFormat.PARQUET,
            bucket=s3.Bucket.from_bucket_arn(self, 'DataBucket', BUCKET_ARN),
            s3_prefix='reddit/',
        )

        # role assumed by firehose
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
                resources=[
                    glue_table.table_arn, 
                    glue_db.database_arn, 
                    glue_db.catalog_arn,
                ],
                actions=[
                    'glue:GetTable',
                    'glue:GetTableVersion',
                    'glue:GetTableVersions',
                ],
            )
        )

        # cloudwatch statement
        stream_role.add_to_policy(
            iam.PolicyStatement(
                resources=['*'],
                actions=[
                    'logs:PutLogEvents',
                ],
            )
        )

        data_format_conversion_configuration = kf.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
            enabled=True,
            input_format_configuration=kf.CfnDeliveryStream.InputFormatConfigurationProperty(
                deserializer=kf.CfnDeliveryStream.DeserializerProperty(
                    hive_json_ser_de=kf.CfnDeliveryStream.HiveJsonSerDeProperty(),
                ),
            ),
            output_format_configuration=kf.CfnDeliveryStream.OutputFormatConfigurationProperty(
                serializer=kf.CfnDeliveryStream.SerializerProperty(
                    parquet_ser_de=kf.CfnDeliveryStream.ParquetSerDeProperty(),
                ),
            ),
            schema_configuration=kf.CfnDeliveryStream.SchemaConfigurationProperty(
                database_name=glue_db.database_name,
                table_name=glue_table.table_name,
                role_arn=stream_role.role_arn,
                region='us-east-2',
            ),
        )

        s3_config = kf.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=BUCKET_ARN,  # temporary, will replace with env variable
            role_arn=stream_role.role_arn,
            data_format_conversion_configuration=data_format_conversion_configuration,
            prefix='reddit/',
            buffering_hints=kf.CfnDeliveryStream.BufferingHintsProperty(
                size_in_m_bs=64,
            ),
        )

        firehose = kf.CfnDeliveryStream(
            self, 'FirehoseStream',
            delivery_stream_name='RedditDataStream',
            extended_s3_destination_configuration=s3_config,
        )

        # add role dependency
        firehose.node.add_dependency(stream_role)

        # add ECS Fargate instance
        app_role = iam.Role(
            self, 'RedditStreamingAppRole',
            assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            description='Role used by the Reddit Streaming Application Fargate Task',
        )

        # add firehose permissions
        app_role.add_to_policy(
            iam.PolicyStatement(
                resources=[firehose.attr_arn],
                actions=[
                    'firehose:DeleteDeliveryStream',
                    'firehose:PutRecord',
                    'firehose:PutRecordBatch',
                    'firehose:UpdateDestination',
                ],
            )
        )

        # add ecs and cloudwatch permissions
        app_role.add_to_policy(
            iam.PolicyStatement(
                resources=['*'],
                actions=[
                    'ecr:GetAuthorizationToken',
                    'ecr:BatchCheckLayerAvailability',
                    'ecr:GetDownloadUrlForLayer',
                    'ecr:BatchGetImage',
                    'logs:CreateLogStream',
                    'logs:PutLogEvents',
                ],
            )
        )

        vpc = ec2.Vpc(self, 'RedditVpc', max_azs=3)

        cluster = ecs.Cluster(self, 'RedditCluster', vpc=vpc)

        task_definition = ecs.FargateTaskDefinition(
            self, 'TaskDefinition',
            memory_limit_mib=512,
            cpu=256,
            task_role=app_role,
        )

        task_definition.add_container(
            id='RedditStreamingApp',
            image=ecs.ContainerImage.from_asset('./sentiment_analysis'),
            command=['all'],
            environment={
                'FIREHOSE_STREAM_NAME': firehose.delivery_stream_name,
                'PRAW_CLIENT_SECRET': os.environ['PRAW_CLIENT_SECRET'],
                'PRAW_CLIENT_ID': os.environ['PRAW_CLIENT_ID'],
                'PRAW_USER_AGENT': os.environ['PRAW_USER_AGENT'],
            },
            logging=ecs.LogDriver.aws_logs(stream_prefix='reddit'),
        )

        container = ecs.FargateService(
            self, 'StreamingApplication',
            desired_count=1,
            task_definition=task_definition,
            cluster=cluster,
            assign_public_ip=True,
        )
