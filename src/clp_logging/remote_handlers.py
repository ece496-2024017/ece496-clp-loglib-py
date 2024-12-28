import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import ClientError
import datetime
import hashlib
import os
import base64


class CLPRemoteHandler():
    """
    Handles CLP file upload and comparison to AWS S3 bucket.
    Configuration of AWS access key is required. Run command `aws configure`
    """

    def __init__(self, log_name, log_path, s3_bucket) -> None:
        self.thread_info = {}
        self.s3_resource = boto3.resource("s3")
        self.s3_client = boto3.client("s3")
        self.log_path = log_path
        self.log_name = log_name
        self.transfer = 0
        self.bucket = s3_bucket

        self.timestamp = datetime.datetime.now()
        self.folder_path = f"logs/{self.timestamp.year}/{self.timestamp.month}/{self.timestamp.day}"

        # print(f"File name: {self.log_name}; File path: {self.log_path}")
        self.obj_key = self._remote_log_naming()
        print(self.obj_key)



        self.multipart_upload_config = {
            "size": 1024 * 1024 * 5,
            "index": 1,
            "pos": 0,
            "uploaded parts": [],
        }

    def _remote_log_naming(self):
        ext = self.log_name.find(".")
        if ext != -1:
            new_filename = f'log_{self.timestamp.strftime("%Y-%m-%d-%H%M%S")}{self.log_name[ext:]}'
        else:
            new_filename = f'{self.timestamp.strftime("%Y-%m-%d-%H%M%S")}_{self.log_name}'
        file_name = f"{self.folder_path}/{new_filename}"
        return file_name

    def _calculate_part_sha256(self, data):
        """Calculate the SHA256 checksum for the provided data."""
        sha256_hash = hashlib.sha256()
        sha256_hash.update(data)
        return base64.b64encode(sha256_hash.digest()).decode('utf-8')


    def _upload_part(self, upload_id):
        # Read the latest file
        try:
            with open(self.log_path, 'rb') as file:
                file.seek(self.multipart_upload_config["pos"])
                upload_data = file.read(self.multipart_upload_config["size"])
        except FileNotFoundError as e:
            print(f"The log file {self.log_path} cannot be found: {e}")
            raise
        except IOError as e:
            print(f"IO Error occurred while reading file {self.log_path}: {e}")
            raise
        except Exception as e:
            print(f"Exception occurred: {e}")
            raise

        try:
            sha256_checksum = self._calculate_part_sha256(upload_data)
            response = self.s3_client.upload_part(
                Bucket=self.bucket,
                Key=self.obj_key,
                Body=upload_data,
                PartNumber=self.multipart_upload_config["index"],
                UploadId=upload_id,
                ChecksumSHA256=sha256_checksum
            )
            print(f"Uploaded Part {self.multipart_upload_config['index']}")
            print(response)

            # Store both ETag and SHA256 for validation
            return {
                "PartNumber": self.multipart_upload_config["index"],
                "ETag": response["ETag"],
                "ChecksumSHA256": response["ChecksumSHA256"],
            }
        except ClientError as e:
            print(f"ClientError occurred during multipart upload on {self.multipart_upload_config['index']}: {e}")
            raise
        except Exception as e:
            print(f"Exception occurred: {e}")
            raise
    def multipart_upload(self):
        print(f"Initiate Multipart Upload of file {self.log_name}")
        create_ret = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key, ChecksumAlgorithm="SHA256")
        upload_id = create_ret["UploadId"]
        file_size = os.path.getsize(self.log_path)
        try:
            while (
                file_size - self.multipart_upload_config["pos"]
                >= self.multipart_upload_config["size"]
            ):
                upload_status = self._upload_part(upload_id)
                print(upload_status)
                self.multipart_upload_config["index"] += 1
                self.multipart_upload_config["pos"] += self.multipart_upload_config["size"]

                # AWS S3 Part number restriction
                if (
                    self.multipart_upload_config["index"] < 1
                    or self.multipart_upload_config["index"] > 10000
                ):
                    # AWS limits multipart upload count to 10000
                    self.s3_client.complete_multipart_upload(
                        Bucket=self.bucket,
                        Key=self.obj_key,
                        UploadId=upload_id,
                        MultipartUpload={
                            "Parts": [
                                {"PartNumber": part["PartNumber"], "ETag": part["ETag"],
                                 "ChecksumSHA256": part["ChecksumSHA256"]}
                                for part in self.multipart_upload_config["uploaded parts"]
                            ]
                        },
                    )
                    create_ret = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key,
                                                                        ChecksumAlgorithm="SHA256")
                    upload_id = create_ret["UploadId"]
                    break
                self.multipart_upload_config["uploaded parts"].append(upload_status)

            # Upload the remaining segment
            if file_size - self.multipart_upload_config["pos"] < self.multipart_upload_config["size"]:
                response = self._upload_part(upload_id)
                self.multipart_upload_config["index"] += 1
                self.multipart_upload_config["uploaded parts"].append(response)

            # Concatenate the parts in ascending part number order
            response = self.s3_client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.obj_key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": part["PartNumber"], "ETag": part["ETag"], "ChecksumSHA256": part["ChecksumSHA256"]}
                        for part in self.multipart_upload_config["uploaded parts"]
                    ]
                },
            )
            print(response)
            print("Complete multipart upload")
            try:
                response = self.s3_client.head_object(Bucket=self.bucket, Key=self.obj_key)
                print("Object metadata:", response)
            except Exception as e:
                print("Object not found:", e)

            return response
        except NoCredentialsError as no_cred_error:
            print(no_cred_error)
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=upload_id
            )