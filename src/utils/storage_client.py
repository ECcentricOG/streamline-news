from google.cloud import storage
from pathlib import Path
from urllib.parse import urlparse

class StorageClient:

    def __init__(self) -> None:
        self._gcs_client = None

    @staticmethod
    def is_gcs_path(path: str) -> bool:
        return str(path).startswith("gs://")

    def _get_gcs_clinet(self):
        if self._gcs_client is None:
            self._gcs_client = storage.Client()
        return self._gcs_client
    
    @staticmethod
    def join_path(base_path:str, *parts:str) -> str:
        base_path = str(base_path).rstrip("/")

        clean_parts = [
            str(part).strip("/")
            for part in parts
        ]
        suffix = "/".join(clean_parts)

        if not suffix:
            return base_path
        return f"{base_path}/{suffix}"

    def exists(self, path: str) -> bool:
        path = str(path)
        if not self.is_gcs_path(path):
            return Path(path).exists()

        parsed = urlparse(path)
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        if not bucket_name:
            raise ValueError(f"Invalid GCS path: {path}")
        bucket = self._get_gcs_clinet().bucket(bucket_name)

        if bucket.blob(blob_name).exists():
            return True
        
        prefix = blob_name.rstrip("/") + "/"

        blobs = self._get_gcs_clinet().list_blobs(
            bucket_name, prefix=prefix, max_results=1
        )
        return next(iter(blobs), None) is not None

    def upload_file(self, local_path:str, destination_path:str):
        if not self.is_gcs_path(destination_path):
            raise ValueError(
                "Destination must be a gs:// GCS path"
            )

        parsed = urlparse(destination_path)

        bucket = self._get_gcs_clinet().bucket(
            parsed.netloc
        )
        blob_name = parsed.path.lstrip("/")
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(local_path)

    def upload_text(self, content:str, destination_path:str, content_type:str = "text/plain"):
        if self.is_gcs_path(destination_path):
            parsed = urlparse(destination_path)
            bucket = self._get_gcs_clinet().bucket(
                parsed.netloc
            )
            blob = bucket.blob(
                parsed.path.lstrip("/")
            )
            blob.upload_from_string(
                content,
                content_type=content_type
            )
            return 
        local_path = Path(destination_path)
        local_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )
        local_path.write_text(
            content,
            encoding="utf-8"
        )

    def download_file(self, source_path:str, local_path:str):
        if not self.is_gcs_path(source_path):
            raise ValueError(
                "Source must be a gs:// GCS path"
            )
        parsed = urlparse(source_path)
        bucket = self._get_gcs_clinet().bucket(
            parsed.netloc
        )
        blob = bucket.blob(
            parsed.path.lstrip("/")
        )
        destination = Path(local_path)
        destination.parent.mkdir(
            parents=True,
            exist_ok=True
        )
        blob.download_to_filename(str(destination))
