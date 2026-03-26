import os
import uuid
import json
from dotenv import load_dotenv
from google.cloud import logging
from enum import Enum
import requests
from logger.app_logger import get_logger
app_logger = get_logger()
load_dotenv()

class LogLevel(Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    DEBUG = "DEBUG"


class GCPLogger:
    _client = None
    @staticmethod
    def initialize(service_account_json:str):
        """
        Initializes the Google Cloud Logging client with a service account JSON file.

        Args:
            service_account_json (str): Path to the service account JSON file.
        """
        try:
            if GCPLogger._client is None:
                if GCPLogger._is_running_on_gcp():
                    GCPLogger._client = logging.Client()# ADC - no JSON needed
                else:
                    GCPLogger._client = logging.Client.from_service_account_json(service_account_json)
        except Exception as e:
            # Log the exception details using Python's standard logging
            app_logger.error(f"Failed to initialize GCPLogger: {e}")
            # Optionally, re-raise the exception or handle it as needed
            # raise


    @staticmethod
    def _is_running_on_gcp() -> bool:
        try:
            response = requests.get(
                "http://metadata.google.internal",
                headers={"Metadata-Flavor": "Google"},
                timeout=1
            )
            return response.status_code == 200
        except Exception:
            return False
    @staticmethod
    def log(log_level: LogLevel, log_name: str, data: dict | str,send_mail_alert:bool=False):
        """
        Logs data to Google Cloud Logging with the specified log level and log name.

        Args:
            log_level (LogLevel): The severity of the log.
            log_name (str): The name of the log.
            data (dict | str): The log message or structured data.
            send_mail_alert (bool): Whether to send a mail alert for this log.
        """
        try:
            # Determine environment
            is_prod = os.getenv('ENV', "local") == "prod"

            #local env print log to file
            if not is_prod:
                if log_level == LogLevel.ERROR:
                    app_logger.error({"name": log_name, "data": data})
                elif log_level == LogLevel.CRITICAL:
                    app_logger.critical({"name": log_name, "data": data})
                elif log_level == LogLevel.WARNING:
                    app_logger.warning({"name": log_name, "data": data})
                elif log_level == LogLevel.INFO:
                    app_logger.info({"name": log_name, "data": data})
                elif log_level == LogLevel.DEBUG:
                    app_logger.debug({"name": log_name, "data": data})
                else:
                    app_logger.log(log_level, {"name": log_name, "data": data})
            else:
                GCPLogger.send_log_to_gcp(data, log_name, log_level)

             #send alert mail if needed
            if send_mail_alert:
                from services.helper.mail_templates.log_alert import create_log_alert_html
                from services.helper.mail_sender import MailSender
                from classes.models.settings_meta import SettingsMetaModel
                settings_meta=SettingsMetaModel()
                log_alert_emails=settings_meta.get_log_alert_emails()
                try:
                    html_body = create_log_alert_html(log_level, log_name, data)
                    mail_sender = MailSender()
                    mail_sender.send_mail(
                        subject=f"Log Alert: {log_name} - {log_level.name}",
                        body=f"Log Alert: {log_name} - {log_level.name}\n\n{data}",
                        to_emails=log_alert_emails,
                        html_body=html_body
                    )
                except Exception as e:
                    raise Exception(f"Failed to send log alert email: {e}")

        except Exception as e:
            # Log the exception details using Python's standard logging
            app_logger.error(f"Failed to log to GCP: {e}")

    @staticmethod
    def send_log_to_gcp(data, log_name, log_level):
        MAX_LOG_SIZE = 24 * 1024  # 24KB in bytes

        GCPLogger.initialize('private/unidb-442214-7579bc2c1da6.json')
        if GCPLogger._client is None:
            raise Exception("GCPLogger is not initialized. Call GCPLogger.initialize() first.")

        log_name = f"ads_transfer_" + log_name
        logger = GCPLogger._client.logger(log_name)

        # Handle dictionary data
        if isinstance(data, dict):
            # Custom JSON encoder to handle non-serializable objects
            class CustomEncoder(json.JSONEncoder):
                def default(self, obj):
                    try:
                        # First try the default encoder
                        return json.JSONEncoder.default(self, obj)
                    except TypeError:
                        # If that fails, convert to string
                        return str(obj)

            # Convert the dictionary to a JSON-serializable format
            serializable_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    # Recursively handle nested dictionaries
                    serializable_data[key] = {}
                    for sub_key, sub_value in value.items():
                        try:
                            # Try to keep original format if possible
                            json.dumps({sub_key: sub_value})
                            serializable_data[key][sub_key] = sub_value
                        except (TypeError, OverflowError):
                            # Convert to string if not serializable
                            serializable_data[key][sub_key] = str(sub_value)
                else:
                    try:
                        # Try to keep original format if possible
                        json.dumps({key: value})
                        serializable_data[key] = value
                    except (TypeError, OverflowError):
                        # Convert to string if not serializable
                        serializable_data[key] = str(value)

            # Check size of encoded data
            data_str = json.dumps(serializable_data, cls=CustomEncoder)

            if len(data_str.encode('utf-8')) > MAX_LOG_SIZE:
                log_id = str(uuid.uuid4())[:8]

                # Split the dictionary into smaller parts
                keys = list(serializable_data.keys())
                total_parts = 0
                current_part = {}
                part_num = 1

                for key in keys:
                    # Check if this single key-value pair is too large
                    single_item = {key: serializable_data[key]}
                    single_item_size = len(json.dumps(single_item, cls=CustomEncoder).encode('utf-8'))

                    # If a single key-value pair is too large, split it
                    if single_item_size > MAX_LOG_SIZE:
                        # Convert value to string if it's not already
                        if not isinstance(serializable_data[key], str):
                            value_str = json.dumps(serializable_data[key], cls=CustomEncoder)
                        else:
                            value_str = serializable_data[key]

                        # Calculate how many chunks we need for this value
                        encoded_value = value_str.encode('utf-8')

                        # Reserve space for metadata in each chunk
                        metadata_size = len(json.dumps({
                            'part_num': 1,
                            'log_id': log_id,
                            'key': key,
                            'chunk_num': 1,
                            'total_chunks': 1,
                            'value': ''
                        }, cls=CustomEncoder).encode('utf-8'))

                        chunk_size = MAX_LOG_SIZE - metadata_size
                        total_chunks = (len(encoded_value) + chunk_size - 1) // chunk_size

                        # Send each chunk
                        for chunk_num in range(1, total_chunks + 1):
                            start_idx = (chunk_num - 1) * chunk_size
                            end_idx = min(chunk_num * chunk_size, len(encoded_value))

                            chunk = encoded_value[start_idx:end_idx].decode('utf-8', errors='ignore')

                            chunk_part = {
                                'part_num': part_num,
                                'log_id': log_id,
                                'key': key,
                                'chunk_num': chunk_num,
                                'total_chunks': total_chunks,
                                'value': chunk
                            }

                            logger.log_struct(chunk_part, severity=log_level.value)
                            total_parts += 1
                            part_num += 1
                    else:
                        # Try adding this key to the current part
                        log_part = current_part.copy()
                        log_part[key] = serializable_data[key]
                        log_part['part_num'] = part_num
                        log_part['log_id'] = log_id

                        # Check if adding this key exceeds the size limit
                        if len(json.dumps(log_part, cls=CustomEncoder).encode('utf-8')) > MAX_LOG_SIZE:
                            # If current part already has data, send it
                            if current_part:
                                logger.log_struct(current_part, severity=log_level.value)
                                total_parts += 1

                            # Start a new part with just this key
                            part_num += 1
                            current_part = {
                                'part_num': part_num,
                                'log_id': log_id,
                                key: serializable_data[key]
                            }
                        else:
                            # This key fits, add it to current part
                            current_part = log_part

                # Send the last part if it has data
                if current_part:
                    logger.log_struct(current_part, severity=log_level.value)
                    total_parts += 1
            else:
                # Data fits within size limit, log it normally
                logger.log_struct(serializable_data, severity=log_level.value)

        # Handle string/text data
        else:
            data_str = str(data)
            encoded_data = data_str.encode('utf-8')

            if len(encoded_data) > MAX_LOG_SIZE:
                log_id = str(uuid.uuid4())[:8]

                # Calculate effective chunk size (leaving room for part info)
                part_info_size = len(f"Part X/Y (ID: {log_id}): ".encode('utf-8'))
                chunk_size = MAX_LOG_SIZE - part_info_size

                # Calculate total parts
                total_size = len(encoded_data)
                total_parts = (total_size + chunk_size - 1) // chunk_size

                # Split and send each part
                for part_num in range(1, total_parts + 1):
                    start_idx = (part_num - 1) * chunk_size
                    end_idx = min(part_num * chunk_size, total_size)

                    chunk = encoded_data[start_idx:end_idx].decode('utf-8', errors='ignore')
                    part_prefix = f"Part {part_num}/{total_parts} (ID: {log_id}): "

                    logger.log_text(part_prefix + chunk, severity=log_level.value)
            else:
                # Data fits within size limit, log it normally
                logger.log_text(data_str, severity=log_level.value)



