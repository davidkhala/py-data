from typing import Iterable

from davidkhala.gcp.auth import CredentialsInterface, ServiceAccountInfo
from davidkhala.gcp.auth.options import from_service_account, ServiceAccount
from pyarrow import NativeFile, RecordBatch, Table
from pyarrow.fs import GcsFileSystem, FileInfo

from davidkhala.data.format.arrow.fs import FS


class GCS(FS):
    """
    https://arrow.apache.org/docs/python/generated/pyarrow.fs.GcsFileSystem.html
    """

    def __init__(self, public_bucket: bool = False, *, location='ASIA-EAST2', credentials: CredentialsInterface = None):
        options = {
            'anonymous': public_bucket,
            'default_bucket_location': location,
        }
        if credentials:
            options['access_token'] = credentials.token
            options['credential_token_expiration'] = credentials.expiry
        self.fs = GcsFileSystem(**options)

    @staticmethod
    def from_service_account(info: ServiceAccountInfo):
        service_account = from_service_account(info)
        ServiceAccount.token.fget(service_account)  # credential validation included
        return GCS(credentials=service_account.credentials)

    def ls(self, bucket: str) -> FileInfo | list[FileInfo]:
        return super().ls(bucket)

    def write_stream(self, uri, tables_or_batches: Iterable[RecordBatch | Table]):
        """
        pyarrow.lib.ArrowNotImplementedError: Append is not supported in GCS
        :param uri:
        :param tables_or_batches:
        :return:
        """
        if uri.startswith('gs://'):
            uri = uri[5:]

        with self.fs.open_output_stream(uri) as stream:
            FS.write_stream(stream, tables_or_batches)

    def write_batch(self, uri, table_or_batch: RecordBatch | Table):
        """
        pyarrow.lib.ArrowNotImplementedError: Append is not supported in GCS
        :param uri:
        :param table_or_batch:
        :return:
        """
        if uri.startswith('gs://'):
            uri = uri[5:]

        with self.fs.open_output_stream(uri) as stream:
            FS.write_batch(stream, table_or_batch)
