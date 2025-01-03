import boto3
from botocore.exceptions import NoCredentialsError
from clp_logging.handlers import *
from typing import Callable, ClassVar, Dict, IO, Optional, Tuple, Union, List, Any
from pathlib import Path
import datetime
import hashlib
import os
import base64


class CLPRemoteHandler(CLPFileHandler):
    """
    Handles CLP file upload and comparison to AWS S3 bucket.
    Configuration of AWS access key is required. Run command `aws configure`
    """

    def __init__(
            self,
            s3_bucket: str,
    ) -> None:
        self.s3_resource: boto3.resources.factory.s3.ServiceResource = boto3.resource("s3")
        self.s3_client: boto3.client = boto3.client("s3")

        self.bucket: str = s3_bucket
        self.remote_file_count: int = 0

        self.log_name: Optional[str] = None
        self.log_path: Optional[Path] = None
        self.remote_folder_path: Optional[str] = None
        self.obj_key: Optional[str] = None


        self.multipart_upload_config: Dict[str, int] = {
            "size": 1024 * 1024 * 5,
            "index": 1,
            "pos": 0,
        }
        self.uploaded_parts: List[Dict[str, int | str]] = []
        self.upload_id: Optional[int] = None
        self.upload_in_progress: bool = False
        # self.closed: bool = False

    def _remote_log_naming(self, timestamp: datetime.datetime) -> str:
        new_filename: str
        ext: int = self.log_name.find(".")
        upload_time: str = timestamp.strftime("%Y-%m-%d-%H%M%S")
        if self.remote_file_count != 0:
            upload_time += "-" + str(self.remote_file_count)

        if ext != -1:
            new_filename = f'log_{upload_time}{self.log_name[ext:]}'
        else:
            new_filename = f'{upload_time}_{self.log_name}'
        new_filename = f"{self.remote_folder_path}/{new_filename}"
        return new_filename

    def _calculate_part_sha256(self, data: bytes) -> str:
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

    def get_obj_key(self) -> str:
        return self.obj_key

    def set_obj_key(self, obj_key) -> None:
        self.obj_key = obj_key

    def initiate_upload(self, log_path: Path) -> None:
        if self.upload_in_progress:
            raise Exception("An upload is already in progress. Cannot initiate another upload.")

        self.log_path: Path = log_path
        self.log_name: str = log_path.name
        self.upload_in_progress = True
        timestamp: datetime.datetime = datetime.datetime.now()
        self.remote_folder_path: str = f"logs/{timestamp.year}/{timestamp.month}/{timestamp.day}"

        self.obj_key: str = self._remote_log_naming(timestamp)
        create_ret: Dict[str, Any] = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key,
                                                                            ChecksumAlgorithm="SHA256")
        self.upload_id = create_ret["UploadId"]

    def complete_upload(self) -> None:
        if not self.upload_id:
            raise Exception("No upload process to complete.")

        file_size: int = self.log_path.stat().st_size
        try:
            # Upload the remaining segment
            if file_size - self.multipart_upload_config["pos"] < self.multipart_upload_config["size"]:
                self.multipart_upload_config["size"] = file_size - self.multipart_upload_config["pos"]
                upload_status: Dict[str, int | str] = self._upload_part(self.upload_id)
                self.multipart_upload_config["index"] += 1
                self.uploaded_parts.append(upload_status)
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=self.upload_id
            )
            raise e

        print(self.obj_key)
        response = self.s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.obj_key,
            UploadId=self.upload_id,
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
        self.upload_in_progress = False
        self.upload_id = None
        self.obj_key = None



    def multipart_upload(self) -> None:
        # Upload initiation is required before upload
        if not self.upload_id:
            raise Exception("No upload process.")

        file_size: int = self.log_path.stat().st_size
        print(file_size)
        try:
            while (
                file_size - self.multipart_upload_config["pos"]
                >= self.multipart_upload_config["size"]
            ):
                upload_status: Dict[str, int | str] = self._upload_part(self.upload_id)
                print(upload_status)
                self.multipart_upload_config["index"] += 1
                self.multipart_upload_config["pos"] += self.multipart_upload_config["size"]
                self.uploaded_parts.append(upload_status)

                # AWS S3 limits object part count to 10000
                if self.multipart_upload_config["index"] > 10000:
                    self.complete_upload()

                    # Initiate multipart upload to a new S3 object
                    self.remote_file_count += 1
                    self.obj_key = self._remote_log_naming()
                    self.multipart_upload_config["index"] = 1
                    self.uploaded_parts = []
                    create_ret = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key,
                                                                                        ChecksumAlgorithm="SHA256")
                    self.upload_id = create_ret["UploadId"]

        except NoCredentialsError as e:
            raise e
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=self.upload_id
            )
            raise e

    def timeout(self, log_path: Path) -> None:
        print("time out start")
        if not self.upload_id:
            super().__init__(fpath=log_path)
            self.initiate_upload(log_path)

        self.multipart_upload()
        print("time out end")

    def close(self):
        print("close start")
        super().close()
        if self.closed:
            self.complete_upload()
        print("close end")