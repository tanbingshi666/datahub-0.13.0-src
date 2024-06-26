from typing import IO

import yaml

from datahub.configuration import ConfigurationMechanism


class YamlConfigurationMechanism(ConfigurationMechanism):
    """Ability to load configuration from yaml files"""

    def load_config(self, config_fp: IO) -> dict:
        """
        解析配置文件
        """
        return yaml.safe_load(config_fp)
