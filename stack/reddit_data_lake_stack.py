from aws_cdk import (
    core,
    aws_kinesisfirehose as kf,
    aws_iam as iam,
)

class RedditDataLakeStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        role = iam.Role(
            self, 'FirehoseRole',
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com')
        )

        s3_config = kf.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn='arn:aws:s3:::reddit-data-lake-target',  # temporary, will replace with env variable
            role_arn=role.role_arn,
            prefix='reddit/',
        )

        firehose = kf.CfnDeliveryStream(
            self, 'FirehoseStream',
            delivery_stream_name='RedditDataStream',
            s3_destination_configuration=s3_config,
        )
