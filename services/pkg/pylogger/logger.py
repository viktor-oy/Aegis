import logging
import json
import os
import sys

class AegisDevFormatter(logging.Formatter):
    def __init__(self, instance_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instance_id = instance_id

    def format(self, record):
        # Extract standard keys from extra if they exist
        component = getattr(record, 'component', None)
        event = getattr(record, 'event', None)
        
        # In python, record.created is the timestamp in seconds.
        # However, formatTime gives us a string which we can use. 
        # But we'd need datefmt set in __init__ if we want to change the format. 
        # Using self.formatTime is sufficient.
        time_str = self.formatTime(record, "%Y-%m-%d %H:%M:%S") + f".{int(record.msecs):03d}"
        levelname = "WARN" if record.levelname == "WARNING" else record.levelname
        level_str = f"[{levelname:<5}]"
        
        prefix = f"{time_str} {level_str}"
        if self.instance_id:
            prefix += f" [{self.instance_id}]"
            
        if component:
            prefix += f" [{component.upper():<15}]"
        if event:
            prefix += f" [{event.upper():<15}]"

        msg = f'msg="{record.getMessage()}"'
        
        # Add remaining extra fields to the message
        # We need to filter out standard logging record attributes
        standard_attrs = {'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename', 'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated', 'thread', 'threadName', 'processName', 'process', 'message', 'component', 'event'}
        
        extras = []
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith('_'):
                extras.append(f'{key}="{value}"')

        if extras:
            msg += " " + " ".join(extras)

        return f"{prefix} | {msg}"

class AegisJsonFormatter(logging.Formatter):
    def __init__(self, instance_id=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instance_id = instance_id

    def format(self, record):
        log_record = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S") + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "msg": record.getMessage()
        }
        
        component = getattr(record, 'component', None)
        if component:
            log_record["component"] = component
            
        event = getattr(record, 'event', None)
        if event:
            log_record["event"] = event
            
        if self.instance_id:
            log_record["instance_id"] = self.instance_id
        
        standard_attrs = {'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename', 'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated', 'thread', 'threadName', 'processName', 'process', 'message', 'component', 'event'}
        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith('_'):
                log_record[key] = value

        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
            
        return json.dumps(log_record)

def setup_logger(debug=False):
    instance_id = os.environ.get("AEGIS_CP_ID") or os.environ.get("AEGIS_WORKER_ID")
    if not instance_id:
        import socket
        instance_id = socket.gethostname()

    level = logging.DEBUG if debug else logging.INFO
    root_logger = logging.getLogger()
    
    # Remove any existing handlers
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    
    fmt = os.environ.get("LOG_FORMAT", "text")
    if fmt.lower() == "json":
        handler.setFormatter(AegisJsonFormatter(instance_id=instance_id))
    else:
        handler.setFormatter(AegisDevFormatter(instance_id=instance_id))
        
    root_logger.addHandler(handler)
    return root_logger

import os
logger = setup_logger(os.environ.get("AEGIS_DEBUG", "false").lower() == "true")
