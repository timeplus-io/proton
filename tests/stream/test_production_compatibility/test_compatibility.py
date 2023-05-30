from helpers import tpexporter
import os
import sys
import time
import logging
from logging import fatal
import pytest
import math

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.tpexporter import tpexporter
#from helpers.tpexporter import TPExporter, OperateTrackingType



cur_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = f"{cur_dir}/configs/config.json"
# tests_file_path = f"{cur_dir}/tests.json"
tests_file_path = f"{cur_dir}"
docker_compose_file_path = f"{cur_dir}/configs/docker-compose.yaml"


@pytest.fixture(scope='module')
def tp_exporter():
    tp_exporter = tpexporter.TPExporter.create_from_config(config_file_path)
    return tp_exporter


@pytest.fixture(scope='function')
def workspace_clean():
    tp_operate_objs = []
    yield tp_operate_objs
    logger.debug(f"workspace_clean: tp_operate_objs = {tp_operate_objs}")
    for tp_operate_obj in tp_operate_objs:
        target_workspaces = tp_operate_obj.target_workspaces
        for target_workspace in target_workspaces:
            logger.debug(
                f"workspace_clean: target_workspace.name = {target_workspace.name}, target_workspace.access_privilege = {target_workspace.access_privilege}")
            if target_workspace.access_privilege == tpexporter.WorkspacePrivilege.WRITE_ONLY or target_workspace.access_privilege == tpexporter.WorkspacePrivilege.READ_WRITE:
                target_workspace.clean_import_dependencies()


def result_check(tp_operate):
    operate_tracking = tp_operate.tracking.get(
        tpexporter.OperateTrackingType.SCHEMA_EXPORT.value)
    source_workspace = tp_operate.source_workspace
    assert operate_tracking != None
    logger.debug(
        f"tp_operate.name = {tp_operate.name}, operate_tracking = {operate_tracking}")
    # operate_tracking is a list of dict, each dict has source_workspace_name, target_workspace_name, totoal_source_obj_2_export_number and total_schema_exported_obj_number, totoal_source_obj_2_export_number and total_schema_exported_obj_number are dicts, we'd compare key-value pairs in totoal_source_obj_2_export_number and total_schema_exported_obj_number

    # tp-duckbill has a view with issue (Code: 80.\nDB::Exception: MaterializedView default.mv_lpn_list_new cannot select from itself), which is not exported
    if source_workspace.name != "tp-duckbill":
        for operate_tracking_item in operate_tracking:
            for key in operate_tracking_item["summary"]["total_source_obj_2_export_number"].keys():
                assert operate_tracking_item["summary"]["total_source_obj_2_export_number"][key] == (
                    operate_tracking_item["summary"]["total_schema_exported_obj_number"][key] + operate_tracking_item["summary"]["total_schema_export_skip_number"][key])
    else:
        for operate_tracking_item in operate_tracking:
            for key in operate_tracking_item["summary"]["total_source_obj_2_export_number"].keys():
                if key == "view":
                    if (operate_tracking_item["summary"]["total_source_obj_2_export_number"][key] - 1) == operate_tracking_item["summary"]["total_schema_exported_obj_number"][key]:
                        return True
                    elif operate_tracking_item["summary"]["total_source_obj_2_export_number"][key] == (operate_tracking_item["summary"]["total_schema_exported_obj_number"][key] + operate_tracking_item["summary"]["total_schema_export_skip_number"][key]):
                        return True
                    else:
                        return False
                else:
                    assert operate_tracking_item["summary"]["total_source_obj_2_export_number"][key] == (
                        operate_tracking_item["summary"]["total_schema_exported_obj_number"][key] + operate_tracking_item["summary"]["total_schema_export_skip_number"][key])

    return True


@pytest.mark.back_proton_compatibility
def test_demo_site(tp_exporter, workspace_clean, caplog):
    tp_operate = tp_exporter.get_operate("demo-site-replica-all-2-local4")
    logger.debug(f"tp_operate = {tp_operate}")
    workspace_clean.append(tp_operate)
    tp_operate.run()
    assert result_check(tp_operate) == True


@pytest.mark.back_proton_compatibility
def test_tp_demo_site(tp_exporter, workspace_clean, caplog):

    tp_operate = tp_exporter.get_operate("tp-demo-replica-all-2-local4")
    logger.debug(f"tp_operate = {tp_operate}")
    workspace_clean.append(tp_operate)
    tp_operate.run()
    assert result_check(tp_operate) == True


@pytest.mark.back_Proton_compatibility
@pytest.mark.skip("skip for now")
def test_duckbill_site(tp_exporter, workspace_clean, caplog):
    tp_operate = tp_exporter.get_operate("tp-duckbill-replica-all-2-local4")
    logger.debug(f"tp_operate = {tp_operate}")
    workspace_clean.append(tp_operate)
    tp_operate.run()
    assert result_check(tp_operate) == True

# @pytest.mark.back_compatibility
# def test_tp_demo(tp_exporter, caplog):
#     tp_exporter.run_exporter("tp-demo-replica-all")


# @pytest.mark.back_compatibility
# def test_duckbill(cp_exporter, caplog):
#     tp_exporter.run_test_suite("duckbill")
