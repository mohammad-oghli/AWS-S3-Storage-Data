import boto3


class StorageS3:
    """
    A class representing aws s3 storage.

    Attributes:
        name (str): The Name of the aws s3 bucket.
        default_region (str): The default region for aws s3 bucket.
        recent_def_region (list): A list containing recent default region of s3 objects.
    """

    recent_def_region = []
    """
        A class attribute to store recent default region of s3 objects
    """

    @classmethod
    def set_default_region(cls, region_name):
        if StorageS3.recent_def_region:
            if StorageS3.recent_def_region[-1] != region_name:
                StorageS3.recent_def_region.pop()
                StorageS3.recent_def_region.append(region_name)
                # Execute the code
                boto3.setup_default_session(region_name=region_name)
                print("New Default AWS Region has been set up")
        else:
            StorageS3.recent_def_region.append(region_name)
            boto3.setup_default_session(region_name=region_name)
            print("Default AWS Region has been set up")

    # The default value for region 'us-east-1'
    # when you don't set default_region attribute of the new S3 object
    # it will create S3 object in 'us-east-1' region
    def __init__(self, name, default_region='us-east-1'):
        """
        Initializes StorageS3 object.

        Parameters:
            name (str): The name of the aws s3 bucket.
            default_region (str): The default region for aws s3 bucket.
        """

        StorageS3.set_default_region(default_region)
        # Default setting to use AWS configure shared credentials in your OS
        # Credentials located by default in ~/.aws/credentials
        # You can also set AWS credentials using OS environment variables
        self.s3_client = boto3.client('s3')
        self.name = name

    def read_file(self, file_path):
        """
        Read file in aws s3 storage.

        Parameters:
            file_path (str): File location in s3 bucket.

        Returns:
            bytes: Object content of the file in byte format.
        """
        response = self.s3_client.get_object(Bucket=self.name, Key=file_path)
        object_content = response['Body'].read()
        return object_content

    def write_file(self, binary_data, output_path):
        """
        Write file in aws s3 storage.

        Parameters:
            binary_data (bytes): Data content to write in new file.
            file_path (str): File location in s3 bucket.
        """
        self.s3_client.put_object(Body=binary_data, Bucket=self.name, Key=output_path)

    # def write_html_file(self, html_data, output_path):
    #     self.s3_client.put_object(Body=html_data, ContentType='text/html', Bucket=self.name, Key=output_path)

    def copy_file(self, src_file_path, dest_bucket, dest_file_path=''):
        """
        Copy file from aws s3 storage.

        Parameters:
            src_file_path (str): The location of the file to copy in source s3 bucket.
            dest_bucket (str): The destination s3 bucket that will store the copied file.
            dest_file_path (str): The location of copied file in destination s3 bucket.
        """
        cp_src = {'Bucket': self.name, 'Key': src_file_path}
        # Copy the object
        self.s3_client.copy_object(
            Bucket=dest_bucket,
            Key=dest_file_path,
            CopySource=cp_src
        )
        print(f"Copied {src_file_path} to {dest_file_path}")

    def delete_file(self, file_path):
        """
        Delete file from aws s3 storage.

        Parameters:
            file_path (str): File location in s3 bucket.
        """
        self.s3_client.delete_object(Bucket=self.name, Key=file_path)

    def get_file_permission(self, file_path):
        """
        Get file access permission policy in aws s3 storage.

        Parameters:
            file_path (str): File location in s3 bucket.

        Returns:
            list: Dict contains access permission policy information of the file.
        """
        response = self.s3_client.get_object_acl(
            Bucket=self.name,
            Key=file_path,
        )
        return response['Grants']

    def set_file_permission(self, file_path, public=False):
        """
        Set file access permission policy in aws s3 storage.

        Parameters:
            file_path (str): File location in s3 bucket.
            public (bool): Access permission type of the file which is public or private access permission.
        """
        if public:
            self.s3_client.put_object_acl(
                ACL="public-read", Bucket=self.name, Key=file_path
            )
        else:
            self.s3_client.put_object_acl(
                ACL="private", Bucket=self.name, Key=file_path
            )
        return self.get_file_permission(file_path)

    def copy_directory_files(self, src_dir_path, dest_bucket, dest_dir_path=''):
        """
        Copy Directory files from aws s3 storage.

        Parameters:
            src_dir_path (str): The location of the directory to copy in source s3 bucket.
            dest_bucket (str): The destination s3 bucket that will store the copied directory files.
            dest_dir_path (str): The location of copied directory in destination s3 bucket.
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        # Paginate through the source directory
        for page in paginator.paginate(Bucket=self.name, Prefix=src_dir_path):
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Construct the source and destination object keys
                    src_key = obj['Key']
                    # Construct the destination key maintaining directory structure
                    dest_key = dest_dir_path + src_key
                    # Copy the object
                    self.s3_client.copy_object(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        CopySource={'Bucket': self.name, 'Key': src_key}
                    )
                    print(f"Copied {src_key} to {dest_key}")

    def set_directory_permission(self, dir_path, public=False):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        acl_permission = "private"
        if public:
            acl_permission = "public-read"
        # Paginate through the source directory
        for page in paginator.paginate(Bucket=self.name, Prefix=dir_path):
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Construct the source object keys
                    src_key = obj['Key']
                    # Set permission for the object
                    self.s3_client.put_object_acl(
                        ACL=acl_permission, Bucket=self.name, Key=src_key
                    )
        print(f"Directory {dir_path} objects set to {acl_permission}")
