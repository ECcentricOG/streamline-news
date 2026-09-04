import logging
import os

LOG_DIR = "logs"

def get_logger(name:str, log_type = "application") -> logging.Logger:
    valid_log_types = (
        "application",
        "database",
        "airflow"
    )

    if log_type not in valid_log_types:
        raise ValueError(
            f"Invalid log_type '{log_type}'. "
            f"Use one of: {valid_log_types}"
        )

    log_directory = os.path.join(LOG_DIR, log_type)
    os.makedirs(log_directory, exist_ok=True)

    logger_name = f"{log_type}.{name}"
    logger = logging.getLogger(logger_name) 
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    log_file = os.path.join(log_directory, f"{log_type}.log")
    file_handler = logging.FileHandler(log_file, encoding="utf8")
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.propagate = False
    return logger
