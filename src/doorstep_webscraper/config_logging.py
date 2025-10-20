# config_logging.py
import logging
import os
from google.cloud import logging as gcp_logging
from dotenv import load_dotenv

load_dotenv()
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = gcp_logging.Client.from_service_account_json(credentials_path)

def setup_logging(location=None, log_level='INFO'):
    """
    Configures and returns a logger for Airbnb scraping processes.

    Sets the logging level, output format, and attaches a console handler. The logger
    can optionally include a location prefix in each log message.

    Args:
        location (str | None): Optional location name to prepend to each log message.
        log_level (str): Minimum logging level as a string (e.g., 'INFO', 'DEBUG', 'WARNING').

    Returns:
        logging.Logger: Configured logger instance for use in the application.
    """
    
    ## Create a logger
    logger = logging.getLogger('airbnb_logger')
    #numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    #logger.setLevel(numeric_level)
    logger.setLevel(logging.INFO)

    ## Create a formatter
    formatter = logging.Formatter(f'{location} | %(message)s')

    # Create a Google Cloud Logging handler
    gcp_handler = gcp_logging.handlers.CloudLoggingHandler(client, name='airbnb_log')
    gcp_handler.setLevel(logging.INFO)
    if location:
        gcp_handler.setFormatter(formatter)

    ## Create a stream handler for optional console output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    if location:
        console_handler.setFormatter(formatter)
    
    ## Remove all handlers associated with the logger
    logger.handlers = []

    ## Add the file and console handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(gcp_handler)

    return logger