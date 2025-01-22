from pyarrow.fs import GcsFileSystem, FileInfo
from pyarrow import NativeFile, RecordBatch

from davidkhala.data.format.arrow.fs import FS
from davidkhala.gcp.auth.options import ServiceAccount


class GCS(FS):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.fs.GcsFileSystem.html
    """

    def __init__(self, public_bucket: bool = False, *, location='ASIA-EAST2', service_account: ServiceAccount = None):
        options = {
            'anonymous': public_bucket,
            'default_bucket_location': location,
        }
        if service_account:
            options['target_service_account'] = service_account.credentials.service_account_email
            options['access_token'] = service_account.token
            options['credential_token_expiration'] = service_account.credentials.expiry
        self.fs = GcsFileSystem(**options)

    def ls(self, bucket: str) -> FileInfo | list[FileInfo]:
        return super().ls(bucket)

    def write(self, uri, record_batch: RecordBatch):
        stream: NativeFile
        with self.fs.open_output_stream(uri) as stream:
            FS.write(stream, record_batch)