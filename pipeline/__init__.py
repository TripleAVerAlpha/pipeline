from typing import List, Dict, Tuple

from loguru import logger
from pipeline.utils import load_param

log_param = load_param('setting/logger.yml')
logger.add(**log_param)
logger.success('\n\n -------- НОВЫЙ ЗАПУСК -------- \n\n')

from pipeline.pipeline import Pipeline






