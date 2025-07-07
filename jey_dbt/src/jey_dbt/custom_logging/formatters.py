import logging
import datetime
import pytz

class CustomFormatter(logging.Formatter):
    """
    Class to format log record timestamp to Singapore time and UTC format.
    """

    def converter(self, timestamp: int):
        """Method to convert epoch time to Singapore timezone.

        Args:
            timestamp (int): epoch time.

        Returns:
            datetime: datetime formatted to Singapore timezone.
        """
        dt = datetime.datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        return dt.astimezone(pytz.timezone('Singapore'))

    def formatTime(self, record: logging.LogRecord, datefmt: str = None):
        """Method to format log record timestamp to provided format if specified else iso format.

        Args:
            record (logging.LogRecord): log record.
            datefmt (str, optional): date format. Defaults to None.

        Returns:
            str: formatted timestamp.
        """
        dt = self.converter(record.created)
        if datefmt:
            formatted_timestamp = dt.strftime(datefmt)
        else:
            try:
                formatted_timestamp = dt.isoformat(timespec='milliseconds')
            except TypeError:
                formatted_timestamp = dt.isoformat()
        return formatted_timestamp


JEY_FORMAT = "[%(asctime)s] name=\"%(name)s\" level=%(levelname)s filename=\"%(filename)s\" lineno=%(lineno)d msg=\"%(message)s\""
JeyFormatter = CustomFormatter(JEY_FORMAT, datefmt="%Y-%m-%d %H:%M:%S,%f UTC%z")
