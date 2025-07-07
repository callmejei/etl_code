import queue
import atexit
import logging
from logging.handlers import QueueHandler, QueueListener

class CustomLogger(logging.getLoggerClass()):
    """
    Class to perform asynchronous logging.
    """

    def addHandler(self, hdlr: logging.Handler, asynchronous: bool = False):
        """
        Add specified handler to logger. Supports both synchronous and asynchronous handlers.
        Set asynchronous to True to add the specified handler as an asynchronous handler.

        Args:
            hdlr (logging.Handler): handler to add to logger.
            asynchronous (bool, optional): flag to indicate whether hdlr should be asynchronous.
                If set to True, then hdlr will be asynchronous. Defaults to False.
        """
        if asynchronous:
            if not hasattr(self, 'queue'):
                self.queue = queue.Queue(-1)

            if not hasattr(self, 'queue_handler'):
                self.queue_handler = QueueHandler(self.queue)
                super().addHandler(self.queue_handler)

            if not hasattr(self, 'listener'):
                self.listener = QueueListener(self.queue)
                self.listener.start()
                atexit.register(self._stop_listener)

            self.listener.handlers = self.listener.handlers + (hdlr,)
            return

        return super().addHandler(hdlr)

    def _stop_listener(self):
        """
        Method to shutdown QueueListener if there is one running.
        """
        if hasattr(self, 'listener'):
            self.listener.stop()

def JeyLogger(name: str, *args, **kwargs):
    """
    Function to initialize CustomLogger object. 
    Calls logging.getLogger underneath the hood.

    Args:
        name (str): logger name.

    Returns:
        CustomLogger: custom logger object.
    """
    logging.setLoggerClass(CustomLogger)
    logger = logging.getLogger(name, *args, **kwargs)
    logger.setLevel(logging.DEBUG)
    return logger
