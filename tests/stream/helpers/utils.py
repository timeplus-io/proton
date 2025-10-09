import os, sys, subprocess
import logging, logging.config


cur_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_path)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the log level to DEBUG to capture all messages
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def compose_up(compose_file_path):
    logger.debug(f"compose_up: compose_file_path = {compose_file_path}")
    try:
        cmd = f"docker-compose -f {compose_file_path} up -d"
        logger.debug(f"compose_up: cmd = {cmd}")
        res = subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
        logger.debug(f"compose_up: command output = {res.decode('utf-8')}")
        return True
    except subprocess.CalledProcessError as error:
        logger.error(f"compose_up: command failed with error: {error.output.decode('utf-8')}")
        return False
    except Exception as e:
        logger.error(f"compose_up: an unexpected error occurred: {str(e)}")
        return False