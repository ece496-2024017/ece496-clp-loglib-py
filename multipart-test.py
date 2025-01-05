import logging
from pathlib import Path
from src.clp_logging.handlers import CLPFileHandler, CLPLogLevelTimeout
from src.clp_logging.remote_handlers import CLPRemoteHandler
s3_bucket = "ictrl-test2024"

# # File around 270KB
log_path2 = "logs\ictrl_2024-11-22-17-05.clp.zst"
#
# # File around 45MB
# # log_path = "logs\multipart-example.clp.zst"
log_path = "logs\max-20.1.1.720.qdz"
# Create a CLP Remote Handler instance
remote_handler = CLPRemoteHandler(s3_bucket)
remote_handler.initiate_upload(Path(log_path))
# remote_handler.initiate_upload(Path(log_path2))

remote_handler.multipart_upload()
remote_handler.complete_upload()
'''
uploader = CLPRemoteHandler(s3_bucket)
loglevel_timeout = CLPLogLevelTimeout(lambda: uploader.timeout(Path("example.clp.zst")))
clp_handler = CLPFileHandler(Path("example.clp.zst"), loglevel_timeout=loglevel_timeout)
logger = logging.getLogger(__name__)
logger.addHandler(clp_handler)

for i in range (10000):
    logger.info("example warning")
clp_handler.close()
'''
'''
from unittest.mock import patch, MagicMock

with patch("boto3.client") as mock_client:
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.upload_part.side_effect = Exception("Simulated network error")

    # Call your multipart upload function here
    try:
        remote_handler = CLPRemoteHandler(s3_bucket)
        remote_handler.initiate_upload(Path(log_path))
        remote_handler.multipart_upload()
        remote_handler.complete_upload()
    except Exception as e:
        print(f"Error caught: {e}")
'''
