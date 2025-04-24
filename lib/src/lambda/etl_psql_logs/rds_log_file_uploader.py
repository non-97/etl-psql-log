import os
import gzip
from dataclasses import dataclass
import boto3
from aws_lambda_powertools import Logger, Tracer

from rds_log_file_uploader_constants import (
    MULTIPART_THRESHOLD,
    MULTIPART_CHUNKSIZE,
    MAX_CONCURRENCY,
)

logger = Logger()
tracer = Tracer()


@dataclass(frozen=True)
class RdsFileLogUploaderConfig:
    """RdsFileLogUploader 設定値を管理するデータクラス"""

    db_instance_identifier: str
    log_destination_bucket: str
    last_written: int
    object_key: str

    def __post_init__(self) -> None:
        """初期化後のバリデーション"""
        if not self.db_instance_identifier:
            raise ValueError("DbInstanceIdentifier is required")
        if not self.log_destination_bucket:
            raise ValueError("LogDestinationBucket is required")
        if not self.last_written:
            raise ValueError("LastWritten is required")
        if not self.object_key:
            raise ValueError("ObjectKey is required")


class RdsFileLogUploader:
    """RDSログをS3にアップロードするクラス"""

    def __init__(self, config: RdsFileLogUploaderConfig):
        self.config = config
        self.s3_client = boto3.client("s3")
        self.compression_enabled = (
            os.environ.get("ENABLE_COMPRESSION", "false").lower() == "true"
        )

    @tracer.capture_method
    def _compress_file(self, file_path: str) -> bool:
        """
        ファイルをGZIP圧縮

        Args:
            file_path: 圧縮対象のファイルパス

        Returns:
            bool: 圧縮成功時True
        """

        temp_path = f"{file_path}.tmp"
        try:
            original_size = os.path.getsize(file_path)

            # ファイルサイズが0の場合は圧縮をスキップ
            if original_size == 0:
                logger.info(
                    "Skipping compression for empty file",
                    extra={"file_path": file_path},
                )
                return True

            # Lambda関数のメモリサイズの1/8をチャンクサイズとして使用（バイト単位）
            chunk_size = (
                int(os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")) * 1024 * 1024
            ) // 8

            logger.debug(
                "Compressing file with chunks",
                extra={
                    "file_path": file_path,
                    "original_size": original_size,
                    "chunk_size": chunk_size,
                },
            )

            # チャンク単位で圧縮
            with open(file_path, "rb") as f_in:
                with gzip.open(temp_path, "wb", compresslevel=6) as f_out:
                    while True:
                        chunk = f_in.read(chunk_size)
                        if not chunk:
                            break
                        f_out.write(chunk)

            # 圧縮したファイルで元のファイルを置き換え
            os.replace(temp_path, file_path)

            compressed_size = os.path.getsize(file_path)
            logger.info(
                "Successfully compressed file",
                extra={
                    "file_path": file_path,
                    "original_size": original_size,
                    "compressed_size": compressed_size,
                    "compression_ratio": f"{(compressed_size / original_size) * 100:.2f}%",
                },
            )
            return True

        except Exception as e:
            logger.exception(
                "Failed to compress file",
                extra={"file_path": file_path, "error": str(e)},
            )
            return False

        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception as e:
                    logger.warning(
                        "Failed to remove temporary file",
                        extra={"temp_path": temp_path, "error": str(e)},
                    )

    @tracer.capture_method
    def upload_log_file(self, file_path: str) -> bool:
        """
        ログファイルをS3にアップロード

        Args:
            file_path: アップロードするファイルのパス

        Returns:
            bool: アップロード成功時True
        """

        try:
            content_type = "text/plain"

            # 圧縮が有効な場合のみ圧縮処理を実行
            if self.compression_enabled:
                if self._compress_file(file_path):
                    content_type = "application/gzip"
                else:
                    logger.warning("Compression failed, uploading uncompressed file")

            # メタデータの設定
            metadata = {
                "LastWritten": str(self.config.last_written),
                "DbInstanceIdentifier": self.config.db_instance_identifier,
                "Compressed": str(self.compression_enabled).lower(),
            }

            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=self.config.log_destination_bucket,
                Key=self.config.object_key,
                ExtraArgs={
                    "Metadata": metadata,
                    "ContentType": content_type,
                    "ContentEncoding": (
                        "gzip" if self.compression_enabled else "identity"
                    ),
                },
                Config=boto3.s3.transfer.TransferConfig(
                    multipart_threshold=MULTIPART_THRESHOLD,
                    max_concurrency=MAX_CONCURRENCY,
                    multipart_chunksize=MULTIPART_CHUNKSIZE,
                    use_threads=True,
                ),
            )

            logger.info(
                "Successfully uploaded log file to S3",
                extra={
                    "file_path": file_path,
                    "log_destination_bucket": self.config.log_destination_bucket,
                    "object_key": self.config.object_key,
                    "size": os.path.getsize(file_path),
                    "compressed": self.compression_enabled,
                    "metadata": metadata,
                },
            )
            return True

        except Exception as e:
            logger.exception(
                "Failed to upload log file to S3",
                extra={
                    "file_path": file_path,
                    "log_destination_bucket": self.config.log_destination_bucket,
                    "object_key": self.config.object_key,
                    "error": str(e),
                },
            )
            return False
