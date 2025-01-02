from pathlib import Path
from src.clp_logging.remote_handlers import CLPRemoteHandler
s3_bucket = "ictrl-test2024"

# File around 270KB
# log_name2 = "ictrl_2024-11-22-17-05.clp.zst"
# log_path2 = "logs\ictrl_2024-11-22-17-05.clp.zst"

# File around 45MB
log_name = "max-20.1.1.720.qdz"
# log_path = "logs\multipart-example.clp.zst"
log_path = "max-20.1.1.720.qdz"
# Create a CLP Remote Handler instance
remote_handler = CLPRemoteHandler(s3_bucket)
remote_handler.initiate_upload(log_name, Path(log_path))
# remote_handler.initiate_upload(log_name2, Path(log_path2))

remote_handler.multipart_upload()
remote_handler.complete_upload()