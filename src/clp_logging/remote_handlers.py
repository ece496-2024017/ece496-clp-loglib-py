import base64
import datetime
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import NoCredentialsError

from clp_logging.handlers import CLPFileHandler


class CLPRemoteHandler(CLPFileHandler):
    """
    Handles CLP file upload and comparison to AWS S3 bucket. Configuration of
    AWS access key is required. Run command `aws configure`.

    :param s3_bucket: Name of the AWS S3 Bucket where log files are transferred
    """

    def __init__(
        self,
        s3_bucket: str,
    ) -> None:
        self.s3_resource: boto3.resources.factory.s3.ServiceResource = boto3.resource("s3")
        self.s3_client: boto3.client = boto3.client("s3")
        self.bucket: str = s3_bucket

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
        self.remote_file_count: int = 0
        self.upload_in_progress: bool = False

    def _calculate_part_sha256(self, data: bytes) -> str:
        sha256_hash: hashlib._Hash = hashlib.sha256()
        sha256_hash.update(data)
        return base64.b64encode(sha256_hash.digest()).decode("utf-8")

    def _remote_log_naming(self, timestamp: datetime.datetime) -> str:
        if self.log_name is None:
            raise ValueError("No input file.")

        new_filename: str
        ext: int = self.log_name.find(".")
        upload_time: str = timestamp.strftime("%Y-%m-%d-%H%M%S")
        # Naming of multiple remote files from the same local file
        if self.remote_file_count != 0:
            upload_time += "-" + str(self.remote_file_count)

        if ext != -1:
            new_filename = f"log_{upload_time}{self.log_name[ext:]}"
        else:
            new_filename = f"{upload_time}_{self.log_name}"
        new_filename = f"{self.remote_folder_path}/{new_filename}"
        return new_filename

    def _upload_part(self) -> Dict[str, int | str]:
        if self.log_path is None:
            raise ValueError("No input file.")

        upload_data: bytes
        # Read the latest version of the file
        try:
            with open(self.log_path, "rb") as file:
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
                UploadId=self.upload_id,
                ChecksumSHA256=sha256_checksum,
            )

            # Store both ETag and SHA256 for validation
            return {
                "PartNumber": self.multipart_upload_config["index"],
                "ETag": response["ETag"],
                "ChecksumSHA256": response["ChecksumSHA256"],
            }
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=self.upload_id
            )
            raise Exception(
                f'Multipart Upload on Part {self.multipart_upload_config["index"]}: {e}'
            ) from e

    def get_obj_key(self) -> str | None:
        return self.obj_key

    def set_obj_key(self, obj_key: str) -> None:
        self.obj_key = obj_key

    def initiate_upload(self, log_path: Path) -> None:
        if self.upload_in_progress:
            raise Exception("An upload is already in progress. Cannot initiate another upload.")

        self.log_path = log_path
        self.log_name = log_path.name
        self.upload_in_progress = True
        timestamp: datetime.datetime = datetime.datetime.now()
        self.remote_folder_path = f"logs/{timestamp.year}/{timestamp.month}/{timestamp.day}"

        self.obj_key = self._remote_log_naming(timestamp)
        create_ret: Dict[str, Any] = self.s3_client.create_multipart_upload(
            Bucket=self.bucket, Key=self.obj_key, ChecksumAlgorithm="SHA256"
        )
        self.upload_id = create_ret["UploadId"]

    def multipart_upload(self) -> None:
        # Upload initiation is required before multipart_upload
        if not self.upload_id:
            raise Exception("No upload process.")
        if self.log_path is None:
            raise ValueError("No input file.")

        file_size: int = self.log_path.stat().st_size
        try:
            while (
                file_size - self.multipart_upload_config["pos"]
                >= self.multipart_upload_config["size"]
            ):
                # Perform upload and label the uploaded part
                upload_status: Dict[str, int | str] = self._upload_part()
                self.multipart_upload_config["index"] += 1
                self.multipart_upload_config["pos"] += self.multipart_upload_config["size"]
                self.uploaded_parts.append(upload_status)

                # AWS S3 limits object part count to 10000
                if self.multipart_upload_config["index"] >= 10000:
                    self.s3_client.complete_multipart_upload(
                        Bucket=self.bucket,
                        Key=self.obj_key,
                        UploadId=self.upload_id,
                        MultipartUpload={
                            "Parts": [
                                {
                                    "PartNumber": part["PartNumber"],
                                    "ETag": part["ETag"],
                                    "ChecksumSHA256": part["ChecksumSHA256"],
                                }
                                for part in self.uploaded_parts
                            ]
                        },
                    )

                    # Initiate multipart upload to a new S3 object
                    self.remote_file_count += 1
                    timestamp: datetime.datetime = datetime.datetime.now()
                    self.remote_folder_path = (
                        f"logs/{timestamp.year}/{timestamp.month}/{timestamp.day}"
                    )
                    self.obj_key = self._remote_log_naming(timestamp)
                    self.multipart_upload_config["index"] = 1
                    self.uploaded_parts = []
                    create_ret = self.s3_client.create_multipart_upload(
                        Bucket=self.bucket, Key=self.obj_key, ChecksumAlgorithm="SHA256"
                    )
                    self.upload_id = create_ret["UploadId"]

        except NoCredentialsError as e:
            raise e
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=self.upload_id
            )
            raise e

    def complete_upload(self) -> None:
        # Upload initiation is required before complete_upload
        if not self.upload_id:
            raise Exception("No upload process to complete.")
        if self.log_path is None:
            raise ValueError("No input file.")

        file_size: int = self.log_path.stat().st_size
        try:
            # Upload the remaining segment
            if (
                file_size - self.multipart_upload_config["pos"]
                < self.multipart_upload_config["size"]
            ):
                self.multipart_upload_config["size"] = (
                    file_size - self.multipart_upload_config["pos"]
                )
                upload_status: Dict[str, int | str] = self._upload_part()
                self.multipart_upload_config["index"] += 1
                self.uploaded_parts.append(upload_status)
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=self.upload_id
            )
            raise e

        response = self.s3_client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.obj_key,
            UploadId=self.upload_id,
            MultipartUpload={
                "Parts": [
                    {
                        "PartNumber": part["PartNumber"],
                        "ETag": part["ETag"],
                        "ChecksumSHA256": part["ChecksumSHA256"],
                    }
                    for part in self.uploaded_parts
                ]
            },
        )
        print(response)
        print('Complete multipart upload')
        try:
            response = self.s3_client.head_object(Bucket=self.bucket, Key=self.obj_key)
            print('Object metadata:', response)
        except Exception as e:
            print('Object not found:', e)
        self.upload_in_progress = False
        self.upload_id = None
        self.obj_key = None

    def timeout(self, log_path: Path) -> None:
        # Upload latest segment upon CLPLogLevelTimeout
        if not self.upload_id:
            super().__init__(fpath=log_path)
            self.initiate_upload(log_path)

        self.multipart_upload()

    def close(self) -> None:
        super().close()
        if self.closed:
            self.complete_upload()



    def _calculate_sha256(self):
        """Calculate the SHA256 checksum for the entire file."""
        sha256 = hashlib.sha256()
        with open(self.log_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _calculate_multipart_sha256(self):
        """Calculate the SHA256 checksum for a multipart-uploaded file."""
        part_hashes = []
        with open(self.log_path, "rb") as f:
            while True:
                # Read file parts based on configured size
                data = f.read(self.multipart_upload_config["size"])
                if not data:
                    break
                # Compute SHA256 for each part and append its binary digest
                part_hashes.append(hashlib.sha256(data).digest())
        # Combine the part hashes and compute the final SHA256 checksum
        combined_hash = hashlib.sha256(b"".join(part_hashes)).hexdigest()
        return f"{combined_hash}-{len(part_hashes)}"

    def compare_files(self):
        """Compare the local file with remote files on S3."""
        remote_files = []
        continuation_token = None

        # Loop through paginated results to collect remote file keys
        while True:
            if continuation_token:
                files = self.s3_client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=self.remote_folder_path,
                    ContinuationToken=continuation_token
                )
            else:
                files = self.s3_client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=self.remote_folder_path
                )

            if 'Contents' in files:
                remote_files.extend(obj['Key'] for obj in files['Contents'])

            if files.get('IsTruncated'):
                continuation_token = files.get('NextContinuationToken')
            else:
                break

        # Compare the local file hash with each remote file's hash
        for obj in remote_files:
            # Retrieve the S3 object's checksum
            s3_object = self.s3_client.head_object(Bucket=self.bucket, Key=obj)
            print(s3_object)
            s3_checksum = s3_object['Metadata'].get('checksumsha256')

            if not s3_checksum:
                print(f"No ChecksumSHA256 available for object: {obj}")
                continue

            # Calculate the local SHA256 checksum
            local_hash = self._calculate_sha256()
            print(local_hash)

            # Compare checksums
            if local_hash == s3_checksum:
                print(f"File content matches with S3 object: {obj}")
                return False

        print("Local file does not match with any remote file.")
        return True
