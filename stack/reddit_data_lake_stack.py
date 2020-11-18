from aws_cdk import (
    core,
    aws_kinesisfirehose as kf,
    aws_iam as iam,
    aws_glue as glue,
    aws_ecs as ecs,
    aws_ec2 as ec2,
)

BUCKET_ARN = 'arn:aws:s3:::reddit-data-lake-target'

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

        # add ECS Fargate instance
        app_role = iam.Role(
            self, 'RedditStreamingAppRole',
            assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            description='Role used by the Reddit Streaming Application Fargate Task',
        )

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

        vpc = ec2.Vpc(self, 'RedditVpc', max_azs=3)

        cluster = ecs.Cluster(self, 'RedditCluster', vpc=vpc)

        task_definition = ecs.FargateTaskDefinition(
            self, 'TaskDefinition',
            memory_limit_mib=256,
            cpu=256,
            task_role=app_role,
        )

        task_definition.add_container(
            id='RedditStreamingApp',
            image=ecs.ContainerImage.from_asset('./sentiment_analysis'),
            command=['all'],
            environment={
                'FIREHOSE_STREAM_NAME': firehose.delivery_stream_name,
            }
        )

        container = ecs.FargateService(
            self, 'StreamingApplication',
            desired_count=1,
            task_definition=task_definition,
            cluster=cluster,
            assign_public_ip=True,
        )
