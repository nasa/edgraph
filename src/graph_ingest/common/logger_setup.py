import os
import logging
from logging import Logger

def setup_logger(name: str, log_filename: str, level: int = logging.DEBUG, file_level: int = logging.WARNING) -> Logger:
    """
    Set up and return a logger with the specified name and log file.
    Logs are written to /app/logs.
    """
    log_directory = "/app/logs"
    os.makedirs(log_directory, exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers if the logger is already configured
    if not logger.handlers:
        file_handler = logging.FileHandler(os.path.join(log_directory, log_filename))
        file_handler.setLevel(file_level)
        formatter = logging.Formatter("%(asctime)s|%(name)s|%(levelname)s|%(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
