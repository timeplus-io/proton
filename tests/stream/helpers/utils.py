import os, sys, subprocess
import logging, logging.config


cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)


def compose_up(compose_file_path):
    logger.debug(f"compose_up: compose_file_path = {compose_file_path}")
    try:
        cmd = f"docker-compose -f {compose_file_path} up -d"
        logger.debug(f"compose_up: cmd = {cmd}")
        res = subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError) as Error:
        return False