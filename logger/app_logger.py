import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
load_dotenv()

class Logger:
    _instance = None  # Class-level variable for Singleton instance

    def __new__(cls, log_dir="logger/logs", general_log_file="app.log", error_log_file="error.log"):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._init_logger(log_dir, general_log_file, error_log_file)
        return cls._instance

    def _init_logger(self, log_dir, general_log_file, error_log_file):
        """
        Initialize the logger instance.
        """
        # Ensure log directory exists
        os.makedirs(log_dir, exist_ok=True, mode=0o777)

        # Set logger name and level
        self.logger = logging.getLogger("ads_transfer")
        self.logger.setLevel(logging.DEBUG)

        # Define formatter
        log_formatter = logging.Formatter(
            "%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s"
        )
        maxBytes=10*1024*1024
        backupCount=10
        # General log handler (for all log levels)
        general_file_handler = RotatingFileHandler(
            os.path.join(log_dir, general_log_file), maxBytes=maxBytes, backupCount=backupCount
        )
        general_file_handler.setLevel(logging.DEBUG)
        general_file_handler.setFormatter(log_formatter)
        self.logger.addHandler(general_file_handler)

        # Error log handler (for ERROR and CRITICAL levels)
        error_file_handler = RotatingFileHandler(
            os.path.join(log_dir, error_log_file), maxBytes=maxBytes, backupCount=backupCount
        )
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(log_formatter)
        self.logger.addHandler(error_file_handler)

        # Stream handler for console
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(log_formatter)
        self.logger.addHandler(stream_handler)

    def get_logger(self):
        """
        Retrieve the logger instance.
        """
        return self.logger


# Global function to get the logger
def get_logger():
    """
    Global function to get the Singleton logger instance.
    """
    return Logger().get_logger()