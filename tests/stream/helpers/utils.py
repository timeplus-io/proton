import os, sys, subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

def setup_debug_logging():
    """Configure detailed logging for debugging"""
    root_logger = logging.getLogger()
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    root_logger.setLevel(logging.DEBUG)

def verify_docker_environment():
    """Check if docker and docker-compose are properly installed and running"""
    try:
        # Check docker version
        docker_version = subprocess.check_output(["docker", "--version"], stderr=subprocess.STDOUT)
        logger.debug(f"Docker version: {docker_version.decode('utf-8').strip()}")
        
        # Check docker-compose version
        compose_version = subprocess.check_output(["docker-compose", "--version"], stderr=subprocess.STDOUT)
        logger.debug(f"Docker Compose version: {compose_version.decode('utf-8').strip()}")
        
        # Check if docker daemon is running
        docker_info = subprocess.check_output(["docker", "info"], stderr=subprocess.STDOUT)
        logger.debug("Docker daemon is running")
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Docker environment check failed: {e.output.decode('utf-8')}")
        return False

def verify_compose_file(compose_file_path):
    """Verify docker-compose file exists and is valid"""
    try:
        if not os.path.exists(compose_file_path):
            logger.error(f"Docker compose file not found at: {compose_file_path}")
            return False
            
        # Check if the file is readable
        with open(compose_file_path, 'r') as f:
            content = f.read()
            logger.debug(f"Successfully read compose file: {compose_file_path}")
            
        # Validate docker-compose file
        cmd = f"docker-compose -f {compose_file_path} config"
        result = subprocess.check_output(cmd.split(), stderr=subprocess.STDOUT)
        logger.debug("Docker compose file validation successful")
        
        return True
    except Exception as e:
        logger.error(f"Compose file verification failed: {str(e)}")
        return False

def enhanced_compose_up(compose_file_path):
    """Enhanced version of compose_up with better error handling and logging"""
    logger.debug(f"Starting enhanced_compose_up with file: {compose_file_path}")
    
    # First verify docker environment
    if not verify_docker_environment():
        logger.error("Docker environment verification failed")
        return False
        
    # Then verify compose file
    if not verify_compose_file(compose_file_path):
        logger.error("Docker compose file verification failed")
        return False
    
    try:
        # Try to bring down any existing containers first
        down_cmd = f"docker-compose -f {compose_file_path} down"
        logger.debug(f"Running command: {down_cmd}")
        subprocess.run(down_cmd.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True)
        
        # Now bring up the containers
        up_cmd = f"docker-compose -f {compose_file_path} up -d"
        logger.debug(f"Running command: {up_cmd}")
        result = subprocess.run(up_cmd.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE, check=True)
        
        # Check if containers are running
        ps_cmd = f"docker-compose -f {compose_file_path} ps"
        ps_output = subprocess.check_output(ps_cmd.split(), stderr=subprocess.STDOUT)
        logger.debug(f"Docker compose ps output:\n{ps_output.decode('utf-8')}")
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Docker compose command failed with error:\n{e.stderr.decode('utf-8') if e.stderr else ''}")
        logger.error(f"Command output:\n{e.output.decode('utf-8') if e.output else ''}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during docker-compose up: {str(e)}")
        return False
