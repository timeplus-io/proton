import os, sys, time
import logging
from logging import fatal
import pytest
import math

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
    )

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import rockets, tpexporter

cur_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = f"{cur_dir}/configs/config.json"
#tests_file_path = f"{cur_dir}/tests.json"
tests_file_path = f"{cur_dir}"
docker_compose_file_path = f"{cur_dir}/configs/docker-compose.yaml"


@pytest.fixture()
def test_set_up(request):
    pass

@pytest.fixture()
def test_tear_down(request):
    pass 

def test_run(test_set, caplog):
    pass
