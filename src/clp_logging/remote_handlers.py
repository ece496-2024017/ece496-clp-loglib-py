import boto3
from botocore.exceptions import NoCredentialsError
# from botocore.exceptions import ClientError
from typing import Callable, ClassVar, Dict, IO, Optional, Tuple, Union, List, Any
from pathlib import Path
import datetime
import hashlib
import os
import base64


class CLPRemoteHandler():
    """
    Handles CLP file upload and comparison to AWS S3 bucket.
    Configuration of AWS access key is required. Run command `aws configure`
    """

    def __init__(
            self,
            log_name: str,
            log_path: Path,
            s3_bucket: str,
    ) -> None:
        self.s3_resource: boto3.resources.factory.s3.ServiceResource = boto3.resource("s3")
        self.s3_client: boto3.client = boto3.client("s3")
        self.log_name: str = log_name
        self.log_path: Path = log_path
        self.bucket: str = s3_bucket
        self.remote_file_count: int = 0

        self.timestamp: datetime.datetime = datetime.datetime.now()
        self.folder_path: str = f"logs/{self.timestamp.year}/{self.timestamp.month}/{self.timestamp.day}"

        # print(f"File name: {self.log_name}; File path: {self.log_path}")
        self.obj_key: str = self._remote_log_naming()
        print(self.obj_key)

        self.multipart_upload_config: Dict[str, int] = {
            "size": 1024 * 1024 * 5,
            "index": 1,
            "pos": 0,
        }
        self.uploaded_parts: List[Dict[str, int | str]] = []

    def _remote_log_naming(self) -> str:
        new_filename: str
        ext: int = self.log_name.find(".")
        upload_time: str = self.timestamp.strftime("%Y-%m-%d-%H%M%S")
        if self.remote_file_count != 0:
            upload_time += "-" + str(self.remote_file_count)

        if ext != -1:
            new_filename = f'log_{upload_time}{self.log_name[ext:]}'
        else:
            new_filename = f'{upload_time}_{self.log_name}'
        new_filename = f"{self.folder_path}/{new_filename}"
        return new_filename

    def _calculate_part_sha256(self, data) -> str:
        """Calculate the SHA256 checksum for the provided data."""
        sha256_hash: hashlib.Hash = hashlib.sha256()
        sha256_hash.update(data)
        return base64.b64encode(sha256_hash.digest()).decode('utf-8')


    def _upload_part(self, upload_id) -> Dict[str, int | str]:
        upload_data: bytes
        # Read the latest file
        try:
            with open(self.log_path, 'rb') as file:
                file.seek(self.multipart_upload_config["pos"])
                upload_data = file.read(self.multipart_upload_config["size"])
        except FileNotFoundError as e:
            raise FileNotFoundError(f"The log file {self.log_path} cannot be found: {e}") from e
        except IOError as e:
            raise IOError(f"IO Error occurred while reading file {self.log_path}: {e}") from e
        except Exception as e:
            raise e

        try:
            sha256_checksum: str = self._calculate_part_sha256(upload_data)
            response: Dict[str, Any] = self.s3_client.upload_part(
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
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=upload_id
            )
            raise Exception(f"Error occurred during multipart upload on part {self.multipart_upload_config['index']}: {e}") from e

    def _complete_upload(self, upload_id) -> None:
        response = self.s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.obj_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": part["PartNumber"], "ETag": part["ETag"], "ChecksumSHA256": part["ChecksumSHA256"]}
                    for part in self.uploaded_parts
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

    def get_obj_key(self) -> str:
        return self.obj_key

    def set_obj_key(self, obj_key) -> None:
        self.obj_key = obj_key

    def multipart_upload(self) -> None:
        print(f"Initiate Multipart Upload of file {self.log_name}")
        create_ret: Dict[str, Any] = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key, ChecksumAlgorithm="SHA256")
        upload_id: int = create_ret["UploadId"]
        file_size: int = os.path.getsize(self.log_path)
        try:
            while (
                file_size - self.multipart_upload_config["pos"]
                >= self.multipart_upload_config["size"]
            ):
                upload_status: Dict[str, int | str] = self._upload_part(upload_id)
                print(upload_status)
                self.multipart_upload_config["index"] += 1
                self.multipart_upload_config["pos"] += self.multipart_upload_config["size"]
                self.uploaded_parts.append(upload_status)

                # AWS S3 limits object part count to 10000
                if self.multipart_upload_config["index"] > 10000:
                    self._complete_upload(upload_id)

                    # Initiate multipart upload to a new S3 object
                    self.remote_file_count += 1
                    self.obj_key = self._remote_log_naming()
                    self.multipart_upload_config["index"] = 1
                    self.uploaded_parts = []
                    create_ret = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key,
                                                                        ChecksumAlgorithm="SHA256")
                    upload_id = create_ret["UploadId"]


            # Upload the remaining segment
            if file_size - self.multipart_upload_config["pos"] < self.multipart_upload_config["size"]:
                self.multipart_upload_config["size"] = file_size - self.multipart_upload_config["pos"]
                upload_status: Dict[str, int | str] = self._upload_part(upload_id)
                self.multipart_upload_config["index"] += 1
                self.uploaded_parts.append(upload_status)

            self._complete_upload(upload_id)

        except NoCredentialsError as e:
            raise e
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=upload_id
            )
            raise e