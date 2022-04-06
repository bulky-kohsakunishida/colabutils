import os
import zipfile
from google.cloud import storage

class GCS_utils:
   def __init__(self, project_id :str, bucket_name: str) -> None:
       self.storage_client = storage.Client(project= project_id)
       self.bucket = self.storage_client.get_bucket(bucket_name)

   def download(self, gcs_dl_path: str, local_name: str, exist_ok = False, unzip_flag = False):
       if not os.path.exists(local_name) or exist_ok:
            blob = self.bucket.blob(gcs_dl_path)
            blob.download_to_filename(local_name)
            print(f"{local_name}をダウンロードしました")
            if(unzip_flag):
                with zipfile.ZipFile(local_name) as zf:
                     zf.extractall()

   def upload(self, gcs_ul_path: str, local_name: str, zip_flag = False):
       if(zip_flag):
            with zipfile.ZipFile(local_name + ".zip", 'w') as zf:
                 zf.write(local_name, compress_type = zipfile.ZIP_DEFLATED)
                 local_name = local_name + ".zip"
       blob = self.bucket.blob(gcs_ul_path)
       blob.upload_from_filename(local_name)
       print(f"{local_name}をアップロードしました")
