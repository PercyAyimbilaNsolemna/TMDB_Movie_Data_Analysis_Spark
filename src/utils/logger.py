"""
Logging utilities for TMDB Movie Data Analysis pipeline.

This module provides a robust logger setup function that works consistently
across notebooks and Docker containers. It configures both file and console
output with standardized formatting for debugging and monitoring pipeline execution.
"""

import os
import sys
import logging


def setup_logger(
    name: str,
    log_file: str,
    level=logging.INFO
):
    """
    Configure and return a logger instance with file and console handlers.

    Creates a logger with the specified name that writes to both a file and
    standard output (console). Useful for distributed systems where logs need
    to be persisted to files while also being visible during execution.

    The function is idempotent - calling it multiple times with the same name
    will return the same logger without adding duplicate handlers.

    Args:
        name (str): Logger name (typically module name like "preprocessing").
                   Used for logger identification and hierarchy.
        log_file (str): Path to the output log file.
                       Directory will be created if it doesn't exist.
                       Example: "/logs/data_cleaning.log"
        level: Logging level (e.g., logging.INFO, logging.DEBUG, logging.ERROR).
              Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger instance with both file and stream handlers.
                       Ready to use for logging messages at the specified level.

    Note:
        - Directory for log_file is created automatically (mkdir -p)
        - Handlers are only added once per logger name to prevent duplicate logs
        - Both file and console output use identical formatting for consistency
        - Timestamps use format: 'YYYY-MM-DD HH:MM:SS' (ISO 8601)

    Example:
        >>> logger = setup_logger("my_module", "/logs/my_module.log")
        >>> logger.info("Processing started")
        >>> logger.error("An error occurred")
    """
    # Create output directory if it doesn't exist
    # This is necessary as logs need a destination path
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Get or create logger with the specified name
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Add handlers only if this logger doesn't already have them
    # Prevents duplicate log entries when function is called multiple times
    if not logger.handlers:
        # Define consistent format for all log messages
        # Includes timestamp, logger name, level, and message for traceability
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # File handler: write logs to specified file
        # Allows persistent logging for debugging and audit trails
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Console handler: write logs to stdout
        # Allows real-time monitoring during execution
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)

        # Register both handlers with the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
