import functools
import logging
import logging.config
import os
import sys
import time
import json
import string
import asyncio
import secrets
import threading
from inspect import currentframe, getframeinfo

logging.config.fileConfig('./logs/logging.conf')
ROUND_PRECISION = 13

def generate_id(length):
    alphabet = string.ascii_letters + string.digits
    order_id = ''.join(secrets.choice(alphabet) for i in range(length))
    return order_id

def clean_finished_tasks(tasks: list):
    try:
        still_running_tasks = []
        for task in tasks:
            if not task.done():
                still_running_tasks.append(task)
        return still_running_tasks
    except Exception as e:
        handle_exception(get_logger('default'), e)
        return tasks

def get_logger(name, api_key=None):
    logger = logging.getLogger(name)

    class logger_wrapper(logging.Logger):
        def __init__(self, logger, api_key=None):
            super().__init__("")
            self.api_key = api_key
            self.name = logger.name
            self.level = logger.level
            self.parent = logger.parent
            self.propagate = logger.propagate
            self.handlers = logger.handlers
            self.disabled = logger.disabled

        def error(self, msg: str, *args, **kwargs) -> None:
            super().error(msg, *args, **kwargs)

        def critical(self, msg: str, *args, **kwargs) -> None:
            super().critical(msg, *args, **kwargs)

        def warning(self, msg: str, *args, **kwargs) -> None:
            super().warning(msg, *args, **kwargs)

    return logger_wrapper(logger, api_key)

def compose_log_msg(frameinfo, comment=""):
    try:
        filename = str(frameinfo.filename)
        filename = filename.split("/")
        if len(filename) > 1:
            filename = filename[-1]
        log_message = f"{filename}:{frameinfo.lineno}"
        if len(comment) > 0:
            log_message += f" - Comment:{comment}"
        return log_message
    except Exception as e:
        handle_exception(get_logger('default'), e)
        return str(e)

#TODO add trace?
def handle_exception(logger, fired_exception, comment=""):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    log_message = f"{fname}:{exc_tb.tb_lineno} - {exc_type}:{fired_exception}"
    if len(comment) > 0:
        log_message += f"\nComment:{comment}"
    print(log_message)
    logger.error(log_message)



class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__