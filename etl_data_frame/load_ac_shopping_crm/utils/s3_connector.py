import boto3
from botocore.exceptions import ClientError


class S3Connector:
    def configure(self):
        try:
            self.s3_client = boto3.client(
                service_name="s3", 
                region_name="ap-southeast-2",
                aws_access_key_id = " AKIA2P7EBZVPG5DZWD6B",
                aws_secret_access_key = "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"
            )
        except Exception as e:
            print(e)
            raise

    def upload_file(
        self, file_name, bucket=None, object_name=None, environment="staging"
    ):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        if bucket is None:
            bucket = "ac-shopping-datalake"

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        try:
            self.s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            self.parent_task.logging.log_error(f"S3 File Upload Failed: {e}")
            return False

        print("Upload successful")
        return True
