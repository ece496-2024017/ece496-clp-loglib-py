from pathlib import Path
from src.clp_logging.remote_handlers import CLPRemoteHandler
s3_bucket = "ictrl-test2024"

# File around 270KB
# log_name = "ictrl_2024-11-22-17-05.clp.zst"
# log_path = "logs\ictrl_2024-11-22-17-05.clp.zst"

# File around 45MB
log_name = "max-20.1.1.720.qdz"
# log_path = "logs\multipart-example.clp.zst"
log_path = "max-20.1.1.720.qdz"
# Create a CLP Remote Handler instance
remote_handler = CLPRemoteHandler(log_name, Path(log_path), s3_bucket)
# print(remote_handler.compare_files())
# Check if the file already exists on the cloud
# if not remote_handler.compare_files():
#     print("Cancel Upload.")
# else:
#     print()
    # Upload the file if it does not exist on the cloud
remote_handler.multipart_upload()