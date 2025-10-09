from shrub.v3.evg_project import EvgProject

from config_generator.etc import utils


def generate():
    tasks = []

    for component in utils.all_components():
        if hasattr(component, 'tasks'):
            component_name = utils.component_name(component)
            print(f' - {component_name}')
            tasks += component.tasks()

    tasks.sort(key=lambda v: v.name)
    yaml = utils.to_yaml(EvgProject(tasks=tasks))
    utils.write_to_file(yaml, 'tasks.yml')
