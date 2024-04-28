import io
import os
import pathlib
import re
import sys
import tempfile
import unittest.mock
from typing import Any, Dict, Mapping, Optional, Set, Union
from urllib import parse

import requests
from expandvars import UnboundVariable, expand

from datahub.configuration.common import ConfigurationError, ConfigurationMechanism
from datahub.configuration.json_loader import JsonConfigurationMechanism
from datahub.configuration.toml import TomlConfigurationMechanism
from datahub.configuration.yaml import YamlConfigurationMechanism

Environ = Mapping[str, str]


def _resolve_element(element: str, environ: Environ) -> str:
    if re.search(r"(\$\{).+(\})", element):
        return expand(element, nounset=True, environ=environ)
    elif element.startswith("$"):
        try:
            return expand(element, nounset=True, environ=environ)
        except UnboundVariable:
            return element
    else:
        return element


def _resolve_list(ele_list: list, environ: Environ) -> list:
    new_v: list = []
    for ele in ele_list:
        if isinstance(ele, str):
            new_v.append(_resolve_element(ele, environ=environ))
        elif isinstance(ele, list):
            new_v.append(_resolve_list(ele, environ=environ))
        elif isinstance(ele, dict):
            new_v.append(resolve_env_variables(ele, environ=environ))
        else:
            new_v.append(ele)
    return new_v


def resolve_env_variables(config: dict, environ: Environ) -> dict:
    new_dict: Dict[Any, Any] = {}
    for k, v in config.items():
        if isinstance(v, dict):
            new_dict[k] = resolve_env_variables(v, environ=environ)
        elif isinstance(v, list):
            new_dict[k] = _resolve_list(v, environ=environ)
        elif isinstance(v, str):
            new_dict[k] = _resolve_element(v, environ=environ)
        else:
            new_dict[k] = v
    return new_dict


def list_referenced_env_variables(config: dict) -> Set[str]:
    # This is a bit of a hack, but expandvars does a bunch of escaping
    # and other logic that we don't want to duplicate here.

    vars = set()

    def mock_get_env(key: str, default: Optional[str] = None) -> str:
        vars.add(key)
        if default is not None:
            return default
        return "mocked_value"

    mock = unittest.mock.MagicMock()
    mock.get.side_effect = mock_get_env

    resolve_env_variables(config, environ=mock)

    return vars


WRITE_TO_FILE_DIRECTIVE_PREFIX = "__DATAHUB_TO_FILE_"


def _process_directives(config: dict) -> dict:
    def _process(obj: Any) -> Any:
        if isinstance(obj, dict):
            new_obj = {}
            for k, v in obj.items():
                if isinstance(k, str) and k.startswith(WRITE_TO_FILE_DIRECTIVE_PREFIX):
                    # This writes the value to a temporary file and replaces the value with the path to the file.
                    config_option = k[len(WRITE_TO_FILE_DIRECTIVE_PREFIX):]

                    with tempfile.NamedTemporaryFile("w", delete=False) as f:
                        filepath = f.name
                        f.write(v)

                    new_obj[config_option] = filepath
                else:
                    new_obj[k] = _process(v)

            return new_obj
        else:
            return obj

    return _process(config)


def load_config_file(
        config_file: Union[str, pathlib.Path],
        squirrel_original_config: bool = False,
        squirrel_field: str = "__orig_config",
        allow_stdin: bool = False,
        allow_remote: bool = True,  # TODO: Change the default to False.
        resolve_env_vars: bool = True,  # TODO: Change the default to False.
        process_directives: bool = False,
) -> dict:
    config_mech: ConfigurationMechanism
    if allow_stdin and config_file == "-":
        # If we're reading from stdin, we assume that the input is a YAML file.
        # Note that YAML is a superset of JSON, so this will also read JSON files.
        config_mech = YamlConfigurationMechanism()
        raw_config_file = sys.stdin.read()
    else:
        """
        正常情况下编写的 ingest 文件都是 yaml/yml 格式
        """
        config_file_path = pathlib.Path(config_file)
        if config_file_path.suffix in {".yaml", ".yml"}:
            """
            创建 YAML 格式的对象 YamlConfigurationMechanism 专门解析 YAML 文件格式
            """
            config_mech = YamlConfigurationMechanism()
        elif config_file_path.suffix == ".json":
            config_mech = JsonConfigurationMechanism()
        elif config_file_path.suffix == ".toml":
            config_mech = TomlConfigurationMechanism()
        else:
            raise ConfigurationError(
                f"Only .toml, .yml, and .json are supported. Cannot process file type {config_file_path.suffix}"
            )

        """
        解析配置文件的 schema
        """
        url_parsed = parse.urlparse(str(config_file))
        if allow_remote and url_parsed.scheme in (
                "http",
                "https",
        ):  # URLs will return http/https
            # If the URL is remote, we need to fetch it.
            try:
                response = requests.get(str(config_file))
                raw_config_file = response.text
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot read remote file {config_file_path}: {e}"
                ) from e
        else:
            if not config_file_path.is_file():
                raise ConfigurationError(f"Cannot open config file {config_file_path}")
            """
            读取配置文件的内容为字符串
            """
            raw_config_file = config_file_path.read_text()

    """
    配置文件内容字符串转为 IO
    """
    config_fp = io.StringIO(raw_config_file)
    """
    解析配置文件内容
    场景驱动情况下 调用 YamlConfigurationMechanism.load_config() 解析配置文件内容为 dict 对象
    """
    raw_config = config_mech.load_config(config_fp)

    """
    拷贝字段 dict 对象
    """
    config = raw_config.copy()
    if resolve_env_vars:
        """
        如果配置文件依赖于系统环境变量 则需要填充
        """
        config = resolve_env_variables(config, environ=os.environ)
    if process_directives:
        config = _process_directives(config)

    """
    如果 squirrel_original_config = True 则往 config dict 对象添加对应的 KV
    场景驱动情况下 squirrel_original_config = True, squirrel_field = '__raw_config'
    """
    if squirrel_original_config:
        config[squirrel_field] = raw_config

    """
    返回解析配置文件对应的 dict 对象
    """
    return config
