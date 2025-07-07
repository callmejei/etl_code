import logging

from jey_dbt.custom_logging.formatters import CustomFormatter
from jey_dbt.custom_logging.logger import JeyLogger


class ColorfulFormatter(CustomFormatter):
    """
    Colorful formatter for logging CLI outputs and messages.
    """

    grey = "\x1b[38;21m"
    yellow = "\x1b[33;21m"
    red = "\x1b[31;21m"
    green = "\x1b[32m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    formatter = "[%(asctime)s] - {color}%(levelname)s{reset} name=\"%(name)s\" msg=\"{color}%(message)s{reset}\""

    FORMATS = {
        logging.DEBUG: formatter.format(color=grey, reset=reset),
        logging.INFO: formatter.format(color=green, reset=reset),
        logging.WARNING: formatter.format(color=yellow, reset=reset),
        logging.ERROR: formatter.format(color=red, reset=reset),
        logging.CRITICAL: formatter.format(color=bold_red, reset=reset),
    }

    def format(self, record):
        """
        Method to format log record with a colorful levelname

        Args:
            record (logging.LogRecord): log record.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
def register_cli_logger(
    logger_name,
    logging_level=logging.INFO,
) -> logging.Logger:
    """
    Register logger for the CLI output.
    A colorful streamhandler is bound to this logger.

    Args:
        logger_name (str): Name of the logger (to be used for naming Jey logger.)
        logging_level (logging.Level): Logger's level. Default is `logging.INFO`.

    Returns:
        Registered logger
    """
    logger = JeyLogger(name=logger_name)

    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(ColorfulFormatter())
    streamhandler.setLevel(logging_level)
    logger.addHandler(streamhandler)

    logger.setLevel(logging_level)
    return logger
