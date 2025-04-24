import re
import sys
import os
import gzip
import csv
import tempfile
from typing import Dict, Any, Optional
from urllib.parse import unquote_plus
import boto3
import duckdb
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import (
    S3EventBridgeNotificationEvent,
    event_source,
)

logger = Logger()
tracer = Tracer()


class PsqlLogEtl:
    """PostgreSQLログファイルのETL処理"""

    def __init__(self):
        """初期化"""
        self.duckdb_con = None
        self.s3_client = boto3.client("s3")

    @tracer.capture_method
    def extract_path_components(self, s3_key: str) -> Optional[Dict[str, str]]:
        """S3オブジェクトキーからパスコンポーネントを抽出

        Args:
            s3_key (str): S3オブジェクトキー

        Returns:
            Optional[Dict[str, str]]: パスコンポーネント、無効な形式の場合はNone
        """

        # 例: database-1/database-1-instance-1/raw/2025/01/08/07/postgresql.log.2025-01-08-0735.gz
        pattern = r"(.+?)/(.+?)/raw/(\d{4})/(\d{2})/(\d{2})/(\d{2})/postgresql\.log\.(\d{4}-\d{2}-\d{2}-\d{4})\.gz"
        match = re.match(pattern, s3_key)

        if match:
            db_cluster = match.group(1)
            db_instance = match.group(2)
            year = match.group(3)
            month = match.group(4)
            day = match.group(5)
            hour = match.group(6)
            timestamp = match.group(7)

            return {
                "db_cluster": db_cluster,
                "db_instance": db_instance,
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "timestamp": timestamp,
            }

        logger.warning(f"無効なS3オブジェクトキー形式: {s3_key}")
        return None

    @tracer.capture_method
    def generate_output_key(
        self, components: Dict[str, str], log_type: Optional[str] = None
    ) -> str:
        """出力用のS3オブジェクトキーを生成

        Args:
            components (Dict[str, str]): パスコンポーネント
            log_type (Optional[str], optional): ログタイプ. デフォルトはNone.

        Returns:
            str: 出力先S3オブジェクトキー
        """
        # log_typeが指定されていない場合は'all'を使用
        actual_log_type = log_type if log_type else "all"

        # パスの組み立て
        base_path = f"{components['db_cluster']}/{components['db_instance']}/parquet/{actual_log_type}/{components['year']}/{components['month']}/{components['day']}/{components['hour']}"

        # オブジェクト名の組み立て
        if log_type:
            # log_typeが指定されている場合は_{log_type}.parquetを付加
            file_name = (
                f"postgresql.log.{components['timestamp']}_{actual_log_type}.parquet"
            )
        else:
            # log_typeが指定されていない場合は.parquetのみ
            file_name = f"postgresql.log.{components['timestamp']}.parquet"

        return f"{base_path}/{file_name}"

    @tracer.capture_method
    def initialize_duckdb(self) -> None:
        """DuckDBの初期設定"""
        self.duckdb_con = duckdb.connect(database=":memory:")

        # DuckDBが使用するディレクトリを指定
        self.duckdb_con.execute("SET home_directory='/tmp'")

        # httpfs拡張をインストールしてロード（S3アクセスに必要）
        self.duckdb_con.execute("INSTALL httpfs;")
        self.duckdb_con.execute("LOAD httpfs;")

    @tracer.capture_method
    def convert_to_csv(self, source_bucket: str, source_key: str) -> str:
        """ログオブジェクトのCSV形式への変換

        Args:
            source_bucket (str): ソースS3バケット
            source_key (str): ソースS3オブジェクトキー

        Returns:
            str: CSV形式に変換されたファイルのパス
        """

        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_input_file:
            input_path = temp_input_file.name
            # S3からオブジェクトをGET
            self.s3_client.download_fileobj(source_bucket, source_key, temp_input_file)

        # CSV出力用の一時ファイル
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=False
        ) as temp_csv_file:
            csv_path = temp_csv_file.name

        processed_lines = 0
        parsed_records = 0

        logger.info(f"ログファイルのCSV変換を開始: s3://{source_bucket}/{source_key}")

        try:
            # タイムスタンプのパターン
            timestamp_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC")

            # CSVヘッダーを定義
            csv_headers = [
                "log_timestamp",
                "timezone",
                "remote_host",
                "username",
                "database",
                "process_id",
                "log_level",
                "log_type",
                "message",
            ]

            with (
                gzip.open(
                    input_path, "rt", encoding="utf-8", errors="replace"
                ) as in_file,
                open(csv_path, "w", encoding="utf-8", newline="") as csv_file,
            ):
                # CSVライターを初期化
                csv_writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
                csv_writer.writeheader()

                current_line = []

                for line in in_file:
                    processed_lines += 1
                    line = line.rstrip()

                    # 進捗表示
                    if processed_lines % 10000 == 0:
                        logger.debug(f"処理中... {processed_lines:,}行処理済み")

                    # 新しい行がタイムスタンプで始まる場合
                    if timestamp_pattern.match(line):
                        # 前の行グループがある場合、パースして書き込む
                        if current_line:
                            combined_line = " ".join(current_line)
                            parsed_data = self.parse_log_line(combined_line)
                            if parsed_data:
                                csv_writer.writerow(parsed_data)
                                parsed_records += 1
                        current_line = [line]
                    else:
                        # タイムスタンプで始まらない行は前の行に連結
                        if current_line:
                            current_line.append(line.strip())
                        else:
                            # 最初の行がタイムスタンプで始まらない場合
                            timestamp_match = re.search(
                                r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC", line
                            )
                            if timestamp_match:
                                current_line = [line[timestamp_match.start() :]]
                            else:
                                current_line = [line]

                # 最後の行グループを処理
                if current_line:
                    combined_line = " ".join(current_line)
                    parsed_data = self.parse_log_line(combined_line)
                    if parsed_data:
                        csv_writer.writerow(parsed_data)
                        parsed_records += 1

            logger.info(
                f"ログファイルのCSV変換が完了しました。処理行数: {processed_lines:,}行、パース成功: {parsed_records:,}行"
            )

            # 入力用の一時ファイルを削除
            os.unlink(input_path)

            return csv_path

        except Exception as e:
            # エラー発生時は一時ファイルを削除
            if os.path.exists(input_path):
                os.unlink(input_path)
            if os.path.exists(csv_path):
                os.unlink(csv_path)
            logger.exception(f"ログファイルのCSV変換中にエラーが発生: {str(e)}")
            raise

    def parse_log_line(self, line):
        """ログのレコードのパース"""

        pattern = r"(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s([A-Z]+):(?:\[([^\]]*)\]|:?([^:@]*))?(?::(?:\[([^\]]*)\]|([^@]*)))?@(?:\[([^\]]*)\]|([^:\[]*)):?\[(\d+)\]:([A-Z]+):\s*(?:(AUDIT|duration|connection authenticated|connection authorized|connection received|disconnection):\s*)?(.*)"
        match = re.match(pattern, line)

        if match:
            username = match.group(5) if match.group(5) else match.group(6)
            database = match.group(7) if match.group(7) else match.group(8)

            return {
                "log_timestamp": match.group(1),
                "timezone": match.group(2),
                "remote_host": match.group(3) if match.group(3) else match.group(4),
                "username": None if username == "unknown" else username,
                "database": None if database == "unknown" else database,
                "process_id": match.group(9),
                "log_level": match.group(10),
                "log_type": match.group(11),
                "message": match.group(12),
            }

        return None

    @tracer.capture_method(capture_response=False)
    def create_filtered_parquet(
        self, output_s3_path: str, filter_condition: str
    ) -> int:
        """フィルタリングされたParquetファイルを生成

        Args:
            output_s3_path (str): 出力先S3パス
            filter_condition (str): SQLのWHERE句に使用するフィルタ条件

        Returns:
            int: 処理されたレコード数
        """
        result = self.duckdb_con.execute(
            f"""
            COPY (
                SELECT * FROM parsed_logs 
                WHERE {filter_condition}
            )
            TO '{output_s3_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """
        ).fetchone()

        return result[0] if result else 0

    @tracer.capture_method
    def to_parquet(
        self, source_bucket: str, source_key: str, output_bucket: str
    ) -> Dict[str, Any]:
        """ログファイルを処理してParquet形式に変換

        Args:
            source_bucket (str): ソースS3バケット
            source_key (str): ソースS3オブジェクトキー
            output_bucket (str): 出力先S3バケット

        Returns:
            Dict[str, Any]: 処理結果
        """
        logger.info(
            "ログファイルの処理を開始",
            extra={
                "source_bucket": source_bucket,
                "source_key": source_key,
                "output_bucket": output_bucket,
            },
        )

        # パスコンポーネントを抽出
        components = self.extract_path_components(source_key)
        if not components:
            raise ValueError(f"無効なS3オブジェクトキー形式: {source_key}")

        # 出力キーを生成
        output_key = self.generate_output_key(components)

        # DuckDBを初期化
        if not self.duckdb_con:
            self.initialize_duckdb()

        try:
            # ログファイルをCSV形式に変換
            csv_path = self.convert_to_csv(source_bucket, source_key)

            # CSVファイルをDuckDBにロード
            self.duckdb_con.execute(
                f"""
                CREATE OR REPLACE TABLE parsed_logs AS
                SELECT 
                    CAST(log_timestamp AS TIMESTAMP) AS log_timestamp,
                    timezone,
                    remote_host,
                    username,
                    database,
                    CAST(process_id AS INTEGER) AS process_id,
                    log_level,
                    log_type,
                    message
                FROM read_csv('{csv_path}', parallel=True)
            """
            )

            # 処理されたレコード数を取得
            result = self.duckdb_con.execute(
                "SELECT COUNT(*) FROM parsed_logs"
            ).fetchone()
            total_records = result[0] if result else 0

            # 各ログタイプの処理結果を格納するディクショナリの定義
            results = {"total_records": total_records, "output_files": {}}

            # 全ログのParquetファイルを生成
            all_logs_output = f"s3://{output_bucket}/{output_key}"
            self.duckdb_con.execute(
                f"""
                COPY parsed_logs TO '{all_logs_output}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
            )
            results["output_files"]["all"] = all_logs_output
            logger.info(
                "全ログをParquet形式で保存", extra={"output_path": all_logs_output}
            )

            # 各種ログタイプ別のParquetファイルを生成
            log_types = {
                "error": "log_level IN ('ERROR', 'FATAL', 'PANIC')",
                "slow_query": "log_type = 'duration'",
                "audit": "log_type = 'AUDIT'",
                "connection": "log_type IN ('connection authenticated', 'connection authorized', 'connection received', 'disconnection')",
            }

            for log_type, filter_condition in log_types.items():
                output_key = self.generate_output_key(components, log_type)
                output_path = f"s3://{output_bucket}/{output_key}"
                count = self.create_filtered_parquet(output_path, filter_condition)
                results["output_files"][log_type] = output_path
                results[f"{log_type}_count"] = count

            logger.info(
                "ログファイルの処理の完了",
                extra={
                    "total_records": total_records,
                    "error_count": results["error_count"],
                    "slow_query_count": results["slow_query_count"],
                    "audit_count": results["audit_count"],
                    "connection_count": results["connection_count"],
                },
            )

            return results

        except Exception as e:
            logger.exception(
                "Parquetファイル生成中にエラーが発生",
                extra={
                    "error": str(e),
                    "source_bucket": source_bucket,
                    "source_key": source_key,
                },
            )
            raise
        finally:
            # 一時ファイルの削除
            if "csv_path" in locals() and os.path.exists(csv_path):
                os.unlink(csv_path)

    def close(self):
        """DuckDBのクリーンアップ"""
        if self.duckdb_con:
            self.duckdb_con.close()
            self.duckdb_con = None


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@event_source(data_class=S3EventBridgeNotificationEvent)
def lambda_handler(
    event: S3EventBridgeNotificationEvent, context: LambdaContext
) -> Dict[str, Any]:
    """Lambda関数のハンドラー

    Args:
        event (S3EventBridgeNotificationEvent): S3イベント
        context (LambdaContext): Lambda実行コンテキスト

    Returns:
        Dict[str, Any]: 処理結果

    Raises:
        SystemExit: 予期しないエラーが発生した場合
    """
    processor = None

    try:
        processor = PsqlLogEtl()
        results = []

        source_bucket = event.detail.bucket.name
        source_key = unquote_plus(event.detail.object.key)

        logger.info(
            "S3オブジェクトの処理を開始",
            extra={"source_bucket": source_bucket, "source_key": source_key},
        )

        # 同じバケットに出力
        output_bucket = source_bucket

        # ログファイルを処理
        result = processor.to_parquet(source_bucket, source_key, output_bucket)
        results.append(
            {
                "source_bucket": source_bucket,
                "source_key": source_key,
                "output_bucket": output_bucket,
                "processing_result": result,
            }
        )

        return {
            "statusCode": 200,
            "body": {
                "message": "PostgreSQLログファイルのParquet変換が完了しました",
                "processed_files": len(results),
                "results": results,
            },
        }

    except Exception as e:
        logger.exception("予期しないエラーが発生", extra={"error": str(e)})
        # エラーを再スローせずに終了コードで失敗を示す
        sys.exit(1)

    finally:
        # リソースのクリーンアップ
        if processor:
            processor.close()
