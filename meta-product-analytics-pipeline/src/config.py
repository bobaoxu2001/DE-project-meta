"""
Pipeline Configuration Loader
===============================
Loads and provides access to pipeline_config.yaml settings.
"""

import os
import logging
from functools import lru_cache
from typing import Any

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "config", "pipeline_config.yaml"
)


@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict[str, Any]:
    """Load pipeline configuration from YAML file.

    Returns a cached dict; safe to call multiple times.
    """
    config_path = path or CONFIG_PATH
    if not os.path.exists(config_path):
        logger.warning("Config file not found at %s, using defaults", config_path)
        return {}

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.debug("Loaded config from %s", config_path)
    return config


def get(section: str, key: str | None = None, default: Any = None) -> Any:
    """Get a config value by section and optional key.

    Examples:
        get("database", "path")  -> "data/warehouse/..."
        get("data_quality")      -> {"null_threshold": 0.01, ...}
    """
    config = load_config()
    section_data = config.get(section, {})
    if key is None:
        return section_data or default
    return section_data.get(key, default)
