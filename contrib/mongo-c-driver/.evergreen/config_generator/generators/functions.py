from shrub.v3.evg_project import EvgProject

from config_generator.etc import utils


def generate():
    functions = {}

    for component in utils.all_components():
        if hasattr(component, 'functions'):
            component_name = utils.component_name(component)
            print(f' - {component_name}')
            functions.update(component.functions())

    functions = dict(sorted(functions.items()))
    yml = utils.to_yaml(EvgProject(functions=functions))
    utils.write_to_file(yml, 'functions.yml')
