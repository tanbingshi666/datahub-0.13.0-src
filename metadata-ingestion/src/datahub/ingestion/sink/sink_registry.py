import dataclasses
from typing import Type

from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.sink import Sink


def _check_sink_classes(cls: Type[Sink]) -> None:
    assert not dataclasses.is_dataclass(cls), f"Sink {cls} is a dataclass"
    assert cls.get_config_class()
    assert cls.get_report_class()


"""
创建 PluginRegistry 对象
"""
sink_registry = PluginRegistry[Sink](extra_cls_check=_check_sink_classes)
"""
注册 datahub.ingestion.sink.plugins 字符串
"""
sink_registry.register_from_entrypoint("datahub.ingestion.sink.plugins")
