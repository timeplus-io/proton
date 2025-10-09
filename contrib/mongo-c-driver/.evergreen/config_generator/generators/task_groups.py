from shrub.v3.evg_project import EvgProject

from config_generator.etc import utils


def generate():
    task_groups = []

    for component in utils.all_components():
        if hasattr(component, 'task_groups'):
            component_name = utils.component_name(component)
            print(f' - {component_name}')
            task_groups += component.task_groups()

    task_groups.sort(key=lambda v: v.name)
    yaml = utils.to_yaml(EvgProject(task_groups=task_groups))
    utils.write_to_file(yaml, 'task_groups.yml')
