"""Configuration management with YAML support."""

import logging
from pathlib import Path
from typing import Optional

import yaml

from ..core.models import ConflationConfig

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Manages loading and validation of configuration.

    Supports:
    - YAML file loading
    - Default configuration
    - Environment-specific overrides
    """

    @staticmethod
    def load_from_file(config_path: str) -> ConflationConfig:
        """
        Load configuration from YAML file.

        Args:
            config_path: Path to YAML config file

        Returns:
            ConflationConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid
        """
        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        try:
            with open(config_file, 'r') as f:
                config_dict = yaml.safe_load(f)

            if not config_dict:
                logger.warning(f"Empty config file {config_path}, using defaults")
                return ConflationConfig()

            config = ConflationConfig.from_dict(config_dict)
            logger.info(f"Loaded configuration from {config_path}")
            return config

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file: {e}")
        except Exception as e:
            raise ValueError(f"Failed to load config: {e}")

    @staticmethod
    def load_default() -> ConflationConfig:
        """
        Load default configuration.

        Returns:
            ConflationConfig with default values
        """
        logger.info("Using default configuration")
        return ConflationConfig()

    @staticmethod
    def save_to_file(config: ConflationConfig, config_path: str) -> None:
        """
        Save configuration to YAML file.

        Args:
            config: Configuration to save
            config_path: Path to output file
        """
        config_file = Path(config_path)
        config_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert to nested dict structure
        config_dict = {
            'conflation': {
                'idle_interval': config.idle_interval,
                'max_buffer_size': config.max_buffer_size,
            },
            'state': {
                'backend': config.state_backend,
                'checkpoint_interval': config.checkpoint_interval,
                'file': {
                    'state_path': config.state_file_path,
                    'checkpoint_path': config.checkpoint_path,
                },
            },
            'output': {
                'writer': config.output_writer,
                'file_path': config.output_file_path,
                'enable_batching': config.enable_batching,
                'batch_size': config.batch_size,
            },
            'error_handling': {
                'max_clock_skew': config.max_clock_skew,
                'dlq_path': config.dlq_path,
            },
            'logging': {
                'level': config.log_level,
                'format': config.log_format,
            },
        }

        try:
            with open(config_file, 'w') as f:
                yaml.safe_dump(config_dict, f, default_flow_style=False, sort_keys=False)

            logger.info(f"Saved configuration to {config_path}")

        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            raise

    @staticmethod
    def merge_configs(base: ConflationConfig, overrides: dict) -> ConflationConfig:
        """
        Merge configuration overrides into base config.

        Args:
            base: Base configuration
            overrides: Dictionary of overrides

        Returns:
            New ConflationConfig with merged values
        """
        base_dict = base.model_dump()

        # Apply overrides
        for key, value in overrides.items():
            if key in base_dict:
                base_dict[key] = value

        return ConflationConfig(**base_dict)
