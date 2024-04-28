import contextlib
import itertools
import logging
import os
import platform
import shutil
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, cast

import click
import humanfriendly
import psutil

import datahub
from datahub.configuration.common import (
    ConfigModel,
    IgnorableError,
    PipelineExecutionError,
)
from datahub.ingestion.api.committable import CommitPolicy
from datahub.ingestion.api.common import EndOfStream, PipelineContext, RecordEnvelope
from datahub.ingestion.api.pipeline_run_listener import PipelineRunListener
from datahub.ingestion.api.report import Report
from datahub.ingestion.api.sink import Sink, SinkReport, WriteCallback
from datahub.ingestion.api.source import Extractor, Source
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.extractor.extractor_registry import extractor_registry
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.reporting.reporting_provider_registry import (
    reporting_provider_registry,
)
from datahub.ingestion.run.pipeline_config import PipelineConfig, ReporterConfig
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from datahub.telemetry import stats, telemetry
from datahub.utilities.global_warning_util import (
    clear_global_warnings,
    get_global_warnings,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList

logger = logging.getLogger(__name__)


class LoggingCallback(WriteCallback):
    def __init__(self, name: str = "") -> None:
        super().__init__()
        self.name = name

    def on_success(
            self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        logger.debug(
            f"{self.name} sink wrote workunit {record_envelope.metadata['workunit_id']}"
        )

    def on_failure(
            self,
            record_envelope: RecordEnvelope,
            failure_exception: Exception,
            failure_metadata: dict,
    ) -> None:
        logger.error(
            f"{self.name} failed to write record with workunit {record_envelope.metadata['workunit_id']}"
            f" with {failure_exception} and info {failure_metadata}"
        )


class DeadLetterQueueCallback(WriteCallback):
    def __init__(self, ctx: PipelineContext, config: Optional[FileSinkConfig]) -> None:
        if not config:
            config = FileSinkConfig.parse_obj({"filename": "failed_events.json"})
        self.file_sink: FileSink = FileSink(ctx, config)
        self.logging_callback = LoggingCallback(name="failure-queue")
        logger.info(f"Failure logging enabled. Will log to {config.filename}.")

    def on_success(
            self, record_envelope: RecordEnvelope, success_metadata: dict
    ) -> None:
        pass

    def on_failure(
            self,
            record_envelope: RecordEnvelope,
            failure_exception: Exception,
            failure_metadata: dict,
    ) -> None:
        if "workunit_id" in record_envelope.metadata:
            if isinstance(record_envelope.record, MetadataChangeProposalClass):
                mcp = cast(MetadataChangeProposalClass, record_envelope.record)
                if mcp.systemMetadata:
                    if not mcp.systemMetadata.properties:
                        mcp.systemMetadata.properties = {}
                    if "workunit_id" not in mcp.systemMetadata.properties:
                        # update the workunit id
                        mcp.systemMetadata.properties[
                            "workunit_id"
                        ] = record_envelope.metadata["workunit_id"]
                record_envelope.record = mcp
        self.file_sink.write_record_async(record_envelope, self.logging_callback)

    def close(self) -> None:
        self.file_sink.close()


class PipelineInitError(Exception):
    pass


@contextlib.contextmanager
def _add_init_error_context(step: str) -> Iterator[None]:
    """Enriches any exceptions raised with information about the step that failed."""

    try:
        yield
    except Exception as e:
        raise PipelineInitError(f"Failed to {step}: {e}") from e


@dataclass
class CliReport(Report):
    cli_version: str = datahub.nice_version_name()
    cli_entry_location: str = datahub.__file__
    py_version: str = sys.version
    py_exec_path: str = sys.executable
    os_details: str = platform.platform()

    mem_info: Optional[str] = None
    peak_memory_usage: Optional[str] = None
    _peak_memory_usage: int = 0

    disk_info: Optional[dict] = None
    peak_disk_usage: Optional[str] = None
    _initial_disk_usage: int = -1
    _peak_disk_usage: int = 0

    thread_count: Optional[int] = None
    peak_thread_count: Optional[int] = None

    def compute_stats(self) -> None:
        try:
            mem_usage = psutil.Process(os.getpid()).memory_info().rss
            if self._peak_memory_usage < mem_usage:
                self._peak_memory_usage = mem_usage
                self.peak_memory_usage = humanfriendly.format_size(
                    self._peak_memory_usage
                )
            self.mem_info = humanfriendly.format_size(mem_usage)
        except Exception as e:
            logger.warning(f"Failed to compute memory usage: {e}")

        try:
            disk_usage = shutil.disk_usage("/")
            if self._initial_disk_usage < 0:
                self._initial_disk_usage = disk_usage.used
            if self._peak_disk_usage < disk_usage.used:
                self._peak_disk_usage = disk_usage.used
                self.peak_disk_usage = humanfriendly.format_size(self._peak_disk_usage)
            self.disk_info = {
                "total": humanfriendly.format_size(disk_usage.total),
                "used": humanfriendly.format_size(disk_usage.used),
                "used_initally": humanfriendly.format_size(self._initial_disk_usage),
                "free": humanfriendly.format_size(disk_usage.free),
            }
        except Exception as e:
            logger.warning(f"Failed to compute disk usage: {e}")

        try:
            self.thread_count = threading.active_count()
            self.peak_thread_count = max(self.peak_thread_count or 0, self.thread_count)
        except Exception as e:
            logger.warning(f"Failed to compute thread count: {e}")

        return super().compute_stats()


class Pipeline:
    config: PipelineConfig
    ctx: PipelineContext
    source: Source
    extractor: Extractor
    sink: Sink[ConfigModel, SinkReport]
    transformers: List[Transformer]

    def __init__(
            self,
            config: PipelineConfig,
            dry_run: bool = False,
            preview_mode: bool = False,
            preview_workunits: int = 10,
            report_to: Optional[str] = None,
            no_default_report: bool = False,
            no_progress: bool = False,
    ):
        self.config = config
        self.dry_run = dry_run
        self.preview_mode = preview_mode
        self.preview_workunits = preview_workunits
        self.report_to = report_to
        self.reporters: List[PipelineRunListener] = []
        self.no_progress = no_progress
        self.num_intermediate_workunits = 0
        self.last_time_printed = int(time.time())
        self.cli_report = CliReport()

        self.graph = None
        """
        连接 datahub (测试连接)
        """
        with _add_init_error_context("connect to DataHub"):
            if self.config.datahub_api:
                """
                实例化 DataHubGraph 主要完成与 datahub gms 进行测试连接获取 server id (由 gms server 生成返回)
                """
                self.graph = DataHubGraph(self.config.datahub_api)

            telemetry.telemetry_instance.update_capture_exception_context(
                server=self.graph
            )

        """
        Pipeline 上下文信息
        """
        with _add_init_error_context("set up framework context"):
            self.ctx = PipelineContext(
                run_id=self.config.run_id,
                graph=self.graph,
                pipeline_name=self.config.pipeline_name,
                dry_run=dry_run,
                preview_mode=preview_mode,
                pipeline_config=self.config,
            )

        """
        获取 sink 类型 比如 datahub-rest
        """
        sink_type = self.config.sink.type
        with _add_init_error_context(f"find a registered sink for type {sink_type}"):
            """
            根据 sink 类型获取对应的 sink class 比如 DatahubRestSink
            """
            sink_class = sink_registry.get(sink_type)

        with _add_init_error_context(f"configure the sink ({sink_type})"):
            """
            获取 sink 配置
            场景驱动下 sink = DatahubRestSink sink_config = {"server": "http://127.0.0.1:8080"}
            """
            sink_config = self.config.sink.dict().get("config") or {}
            """
            实例化 sink
            场景驱动下 调用 DatahubRestSink 的父类 Sink 的 __init()
            """
            self.sink = sink_class.create(sink_config, self.ctx)
            logger.debug(f"Sink type {self.config.sink.type} ({sink_class}) configured")

            logger.info(f"Sink configured successfully. {self.sink.configured()}")

        # once a sink is configured, we can configure reporting immediately to get observability
        """
        一旦 sink 配置文件(也即根据配置文件创建对应的 sink 类) 则立马配置 report 以便观测
        默认情况下 report_to = datahub
        """
        with _add_init_error_context("configure reporters"):
            self._configure_reporting(report_to, no_default_report)

        """
        获取 source 类型 比如 mysql
        """
        source_type = self.config.source.type
        with _add_init_error_context(
                f"find a registered source for type {source_type}"
        ):
            """
            获取 source 类型对应的 class 逻辑跟 type 类型获取对应的 class 类型
            场景驱动下 source class = MySQLSource (datahub.ingestion.source.sql.mysql:MySQLSource)MySQLSource
            """
            source_class = source_registry.get(source_type)

        with _add_init_error_context(f"configure the source ({source_type})"):
            """
            实例化 source class
            场景驱动下调用 Source 子类 MySQLSource create()
            """
            self.source = source_class.create(
                self.config.source.dict().get("config", {}), self.ctx
            )
            logger.debug(f"Source type {source_type} ({source_class}) configured")
            logger.info("Source configured successfully.")

        """
        获取 source 抽取器类型 默认为 generic
        """
        extractor_type = self.config.source.extractor
        with _add_init_error_context(f"configure the extractor ({extractor_type})"):
            """
            默认情况下 extractor class 为 WorkUnitRecordExtractor
            """
            extractor_class = extractor_registry.get(extractor_type)
            """
            实例化 WorkUnitRecordExtractor
            """
            self.extractor = extractor_class(
                self.config.source.extractor_config, self.ctx
            )

        """
        配置 transforms 如果配置文件存在对应的配置
        """
        with _add_init_error_context("configure transformers"):
            self._configure_transforms()

    def _configure_transforms(self) -> None:
        self.transformers = []
        if self.config.transformers is not None:
            for transformer in self.config.transformers:
                transformer_type = transformer.type
                transformer_class = transform_registry.get(transformer_type)
                transformer_config = transformer.dict().get("config", {})
                self.transformers.append(
                    transformer_class.create(transformer_config, self.ctx)
                )
                logger.debug(
                    f"Transformer type:{transformer_type},{transformer_class} configured"
                )

    def _configure_reporting(
            self, report_to: Optional[str], no_default_report: bool
    ) -> None:
        if report_to == "datahub":
            # we add the default datahub reporter unless a datahub reporter is already configured
            if not no_default_report and (
                    not self.config.reporting
                    or "datahub" not in [x.type for x in self.config.reporting]
            ):
                """
                往 reporting 添加 type:datahub 配置
                """
                self.config.reporting.append(
                    ReporterConfig.parse_obj({"type": "datahub"})
                )
        elif report_to:
            # we assume this is a file name, and add the file reporter
            self.config.reporting.append(
                ReporterConfig.parse_obj(
                    {"type": "file", "config": {"filename": report_to}}
                )
            )

        """
        根据 report type 获取对应的 report class 并实例化
        场景驱动情况下 
        report_type = datahub
        reporter_class = datahub.ingestion.reporting.datahub_ingestion_run_summary_provider:DatahubIngestionRunSummaryProvider
        """
        for reporter in self.config.reporting:
            reporter_type = reporter.type
            reporter_class = reporting_provider_registry.get(reporter_type)
            reporter_config_dict = reporter.dict().get("config", {})
            try:
                """
                实例化 DatahubIngestionRunSummaryProvider 对象并添加 reporters 集合中
                """
                self.reporters.append(
                    reporter_class.create(
                        config_dict=reporter_config_dict,
                        ctx=self.ctx,
                    )
                )
                logger.debug(
                    f"Reporter type:{reporter_type},{reporter_class} configured."
                )
            except Exception as e:
                if reporter.required:
                    raise
                elif isinstance(e, IgnorableError):
                    logger.debug(f"Reporter type {reporter_type} is disabled: {e}")
                else:
                    logger.warning(
                        f"Failed to configure reporter: {reporter_type}", exc_info=e
                    )

    def _notify_reporters_on_ingestion_start(self) -> None:
        """
        默认情况下 reporters 只有一个值 DatahubIngestionRunSummaryProvider
        """
        for reporter in self.reporters:
            try:
                """
                执行对应的 reporter on_start()
                """
                reporter.on_start(ctx=self.ctx)
            except Exception as e:
                logger.warning("Reporting failed on start", exc_info=e)

    def _notify_reporters_on_ingestion_completion(self) -> None:
        for reporter in self.reporters:
            try:
                reporter.on_completion(
                    status="CANCELLED"
                    if self.final_status == "cancelled"
                    else "FAILURE"
                    if self.has_failures()
                    else "SUCCESS"
                    if self.final_status == "completed"
                    else "UNKNOWN",
                    report=self._get_structured_report(),
                    ctx=self.ctx,
                )
            except Exception as e:
                logger.warning("Reporting failed on completion", exc_info=e)

    @classmethod
    def create(
            cls,
            config_dict: dict,
            dry_run: bool = False,
            preview_mode: bool = False,
            preview_workunits: int = 10,
            report_to: Optional[str] = "datahub",
            no_default_report: bool = False,
            no_progress: bool = False,
            raw_config: Optional[dict] = None,
    ) -> "Pipeline":
        """
        将配置文件解析 dict 对象封装为 PipelineConfig 对象
        """
        config = PipelineConfig.from_dict(config_dict, raw_config)

        """
        创建 Pipeline 对象 调用 Pipeline __init__()
        """
        return cls(
            config,
            dry_run=dry_run,
            preview_mode=preview_mode,
            preview_workunits=preview_workunits,
            report_to=report_to,
            no_default_report=no_default_report,
            no_progress=no_progress,
        )

    def _time_to_print(self) -> bool:
        self.num_intermediate_workunits += 1
        current_time = int(time.time())
        if current_time - self.last_time_printed > 10:
            # we print
            self.num_intermediate_workunits = 0
            self.last_time_printed = current_time
            return True
        return False

    def run(self) -> None:
        with contextlib.ExitStack() as stack:
            """
            是否生成内存配置 默认 None
            """
            if self.config.flags.generate_memory_profiles:
                import memray

                stack.enter_context(
                    memray.Tracker(
                        f"{self.config.flags.generate_memory_profiles}/{self.config.run_id}.bin"
                    )
                )

            """
            初始化状态为 unknown
            """
            self.final_status = "unknown"

            """
            通知 reporter ingestion 开始 也即向 datahub gms server 发送一个请求
            """
            self._notify_reporters_on_ingestion_start()
            callback = None
            try:
                callback = (
                    LoggingCallback()
                    if not self.config.failure_log.enabled
                    else DeadLetterQueueCallback(
                        self.ctx, self.config.failure_log.log_config
                    )
                )

                """
                执行 source 的元数据拉取 入口为 source.get_workunits()
                场景驱动情况下 source = MySQLSource
                """
                for wu in itertools.islice(
                        self.source.get_workunits(),
                        self.preview_workunits if self.preview_mode else None,
                ):
                    try:
                        if self._time_to_print() and not self.no_progress:
                            """
                            打印信息 比如如下：
                            Cli report:
                            {'cli_version': '0.13.1.2',
                             'cli_entry_location': '/opt/app/python-3.8.17/lib/python3.8/site-packages/datahub/__init__.py',
                             'py_version': '3.8.17 (default, Mar 28 2024, 09:53:27) \n[GCC 4.8.5 20150623 (Red Hat 4.8.5-44)]',
                             'py_exec_path': '/usr/local/bin/python3',
                             'os_details': 'Linux-3.10.0-1062.el7.x86_64-x86_64-with-glibc2.17',
                             'mem_info': '91.33 MB',
                             'peak_memory_usage': '91.33 MB',
                             'disk_info': {'total': '53.66 GB', 'used': '41.63 GB', 'used_initally': '41.63 GB', 'free': '12.03 GB'},
                             'peak_disk_usage': '41.63 GB',
                             'thread_count': 2,
                             'peak_thread_count': 2}
                            Source (mysql) report:
                            {'num_tables_fetch_sample_values_failed': 0,
                             'num_tables_classification_attempted': 0,
                             'num_tables_classification_failed': 0,
                             'num_tables_classification_found': 0,
                             'info_types_detected': {},
                             'events_produced': 1,
                             'events_produced_per_sec': 0,
                             'entities': {'container': ['urn:li:container:35dca3613014b8a1fc14e882606049b3']},
                             'aspects': {'container': {'containerProperties': 1}},
                             'warnings': {},
                             'failures': {},
                             'soft_deleted_stale_entities': [],
                             'tables_scanned': 1,
                             'views_scanned': 0,
                             'entities_profiled': 0,
                             'filtered': [],
                             'num_view_definitions_parsed': 0,
                             'num_view_definitions_failed_parsing': 0,
                             'num_view_definitions_failed_column_parsing': 0,
                             'view_definitions_parsing_failures': [],
                             'start_time': '2024-04-23 16:34:17.199805 (20.31 seconds ago)',
                             'running_time': '20.31 seconds'}
                            Sink (datahub-rest) report:
                            {'total_records_written': 0,
                             'records_written_per_second': 0,
                             'warnings': [],
                             'failures': [],
                             'start_time': '2024-04-23 16:33:26.336405 (1 minute and 11.18 seconds ago)',
                             'current_time': '2024-04-23 16:34:37.512382 (now)',
                             'total_duration_in_seconds': 71.18,
                             'max_threads': 15,
                             'gms_version': 'v0.13.0',
                             'pending_requests': 0}
                            
                            ⏳ Pipeline running successfully so far; produced 1 events in 20.31 seconds.
                            """
                            self.pretty_print_summary(currently_running=True)
                    except Exception as e:
                        logger.warning(f"Failed to print summary {e}")

                    """
                    如果不是 dry run 则处理工作单元执行 本质上面已经执行 source.get_workunits() 这里没有核心逻辑 只是简单判断赋值而已
                    """
                    if not self.dry_run:
                        self.sink.handle_work_unit_start(wu)
                    try:
                        """
                        如果配置文件配置了 transform 则执行对应的 transform
                        """
                        record_envelopes = self.extractor.get_records(wu)
                        for record_envelope in self.transform(record_envelopes):
                            if not self.dry_run:
                                try:
                                    """
                                    异步发送 metadata 给 datahub gms server
                                    也即发送 HTTP Post 请求 url = ${gms_server:127.0.0.1:8080}/entities?action=ingest
                                    """
                                    self.sink.write_record_async(
                                        record_envelope, callback
                                    )
                                except Exception as e:
                                    # In case the sink's error handling is bad, we still want to report the error.
                                    self.sink.report.report_failure(
                                        f"Failed to write record: {e}"
                                    )

                    except RuntimeError:
                        raise
                    except SystemExit:
                        raise
                    except Exception as e:
                        logger.error(
                            "Failed to process some records. Continuing.",
                            exc_info=e,
                        )
                        # TODO: Transformer errors should cause the pipeline to fail.

                    """
                    收尾操作 source 关闭等
                    """
                    self.extractor.close()
                    if not self.dry_run:
                        self.sink.handle_work_unit_end(wu)
                self.source.close()
                # no more data is coming, we need to let the transformers produce any additional records if they are holding on to state
                for record_envelope in self.transform(
                        [
                            RecordEnvelope(
                                record=EndOfStream(),
                                metadata={"workunit_id": "end-of-stream"},
                            )
                        ]
                ):
                    if not self.dry_run and not isinstance(
                            record_envelope.record, EndOfStream
                    ):
                        # TODO: propagate EndOfStream and other control events to sinks, to allow them to flush etc.
                        self.sink.write_record_async(record_envelope, callback)

                """
                收尾操作 sink 关闭等
                """
                self.sink.close()
                self.process_commits()
                self.final_status = "completed"
            except (SystemExit, RuntimeError, KeyboardInterrupt) as e:
                self.final_status = "cancelled"
                logger.error("Caught error", exc_info=e)
                raise
            finally:
                clear_global_warnings()

                if callback and hasattr(callback, "close"):
                    callback.close()  # type: ignore

                self._notify_reporters_on_ingestion_completion()

    def transform(self, records: Iterable[RecordEnvelope]) -> Iterable[RecordEnvelope]:
        """
        Transforms the given sequence of records by passing the records through the transformers
        :param records: the records to transform
        :return: the transformed records
        """
        for transformer in self.transformers:
            records = transformer.transform(records)

        return records

    def process_commits(self) -> None:
        """
        Evaluates the commit_policy for each committable in the context and triggers the commit operation
        on the committable if its required commit policies are satisfied.
        """
        has_errors: bool = (
            True
            if self.source.get_report().failures or self.sink.get_report().failures
            else False
        )
        has_warnings: bool = bool(
            self.source.get_report().warnings or self.sink.get_report().warnings
        )

        for name, committable in self.ctx.get_committables():
            commit_policy: CommitPolicy = committable.commit_policy

            logger.info(
                f"Processing commit request for {name}. Commit policy = {commit_policy},"
                f" has_errors={has_errors}, has_warnings={has_warnings}"
            )

            if (
                    commit_policy == CommitPolicy.ON_NO_ERRORS_AND_NO_WARNINGS
                    and (has_errors or has_warnings)
            ) or (commit_policy == CommitPolicy.ON_NO_ERRORS and has_errors):
                logger.warning(
                    f"Skipping commit request for {name} since policy requirements are not met."
                )
                continue

            try:
                committable.commit()
            except Exception as e:
                logger.error(f"Failed to commit changes for {name}.", e)
                raise e
            else:
                logger.info(f"Successfully committed changes for {name}.")

    def raise_from_status(self, raise_warnings: bool = False) -> None:
        if self.source.get_report().failures:
            raise PipelineExecutionError(
                "Source reported errors", self.source.get_report()
            )
        if self.sink.get_report().failures:
            raise PipelineExecutionError("Sink reported errors", self.sink.get_report())
        if raise_warnings:
            if self.source.get_report().warnings:
                raise PipelineExecutionError(
                    "Source reported warnings", self.source.get_report()
                )
            if self.sink.get_report().warnings:
                raise PipelineExecutionError(
                    "Sink reported warnings", self.sink.get_report()
                )

    def log_ingestion_stats(self) -> None:
        source_failures = self._approx_all_vals(self.source.get_report().failures)
        source_warnings = self._approx_all_vals(self.source.get_report().warnings)
        sink_failures = len(self.sink.get_report().failures)
        sink_warnings = len(self.sink.get_report().warnings)
        global_warnings = len(get_global_warnings())

        telemetry.telemetry_instance.ping(
            "ingest_stats",
            {
                "source_type": self.config.source.type,
                "sink_type": self.config.sink.type,
                "transformer_types": [
                    transformer.type for transformer in self.config.transformers or []
                ],
                "records_written": stats.discretize(
                    self.sink.get_report().total_records_written
                ),
                "source_failures": stats.discretize(source_failures),
                "source_warnings": stats.discretize(source_warnings),
                "sink_failures": stats.discretize(sink_failures),
                "sink_warnings": stats.discretize(sink_warnings),
                "global_warnings": global_warnings,
                "failures": stats.discretize(source_failures + sink_failures),
                "warnings": stats.discretize(
                    source_warnings + sink_warnings + global_warnings
                ),
            },
            self.ctx.graph,
        )

    def _approx_all_vals(self, d: LossyDict[str, LossyList]) -> int:
        result = d.dropped_keys_count()
        for k in d:
            result += len(d[k])
        return result

    def _get_text_color(self, running: bool, failures: bool, warnings: bool) -> str:
        if running:
            return "cyan"
        else:
            if failures:
                return "bright_red"
            elif warnings:
                return "bright_yellow"
            else:
                return "bright_green"

    def has_failures(self) -> bool:
        return bool(
            self.source.get_report().failures or self.sink.get_report().failures
        )

    def pretty_print_summary(
            self, warnings_as_failure: bool = False, currently_running: bool = False
    ) -> int:
        click.echo()
        click.secho("Cli report:", bold=True)
        click.secho(self.cli_report.as_string())
        click.secho(f"Source ({self.config.source.type}) report:", bold=True)
        click.echo(self.source.get_report().as_string())
        click.secho(f"Sink ({self.config.sink.type}) report:", bold=True)
        click.echo(self.sink.get_report().as_string())
        global_warnings = get_global_warnings()
        if len(global_warnings) > 0:
            click.secho("Global Warnings:", bold=True)
            click.echo(global_warnings)
        click.echo()
        workunits_produced = self.source.get_report().events_produced
        duration_message = f"in {humanfriendly.format_timespan(self.source.get_report().running_time)}."

        if self.source.get_report().failures or self.sink.get_report().failures:
            num_failures_source = self._approx_all_vals(
                self.source.get_report().failures
            )
            num_failures_sink = len(self.sink.get_report().failures)
            click.secho(
                f"{'⏳' if currently_running else ''} Pipeline {'running' if currently_running else 'finished'} with at least {num_failures_source + num_failures_sink} failures{' so far' if currently_running else ''}; produced {workunits_produced} events {duration_message}",
                fg=self._get_text_color(
                    running=currently_running,
                    failures=True,
                    warnings=False,
                ),
                bold=True,
            )
            return 1
        elif (
                self.source.get_report().warnings
                or self.sink.get_report().warnings
                or len(global_warnings) > 0
        ):
            num_warn_source = self._approx_all_vals(self.source.get_report().warnings)
            num_warn_sink = len(self.sink.get_report().warnings)
            num_warn_global = len(global_warnings)
            click.secho(
                f"{'⏳' if currently_running else ''} Pipeline {'running' if currently_running else 'finished'} with at least {num_warn_source + num_warn_sink + num_warn_global} warnings{' so far' if currently_running else ''}; produced {workunits_produced} events {duration_message}",
                fg=self._get_text_color(
                    running=currently_running, failures=False, warnings=True
                ),
                bold=True,
            )
            return 1 if warnings_as_failure else 0
        else:
            click.secho(
                f"{'⏳' if currently_running else ''} Pipeline {'running' if currently_running else 'finished'} successfully{' so far' if currently_running else ''}; produced {workunits_produced} events {duration_message}",
                fg=self._get_text_color(
                    running=currently_running, failures=False, warnings=False
                ),
                bold=True,
            )
            return 0

    def _get_structured_report(self) -> Dict[str, Any]:
        return {
            "cli": self.cli_report.as_obj(),
            "source": {
                "type": self.config.source.type,
                "report": self.source.get_report().as_obj(),
            },
            "sink": {
                "type": self.config.sink.type,
                "report": self.sink.get_report().as_obj(),
            },
        }
