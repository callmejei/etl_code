import logging

from jey_dbt.utils.cli_logging import register_cli_logger

logger = register_cli_logger("cli_logger", logging_level=logging.INFO)
