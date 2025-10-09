from shrub.v3.evg_project import EvgProject

from config_generator.etc import utils


def generate():
    variants = []

    for component in utils.all_components():
        if hasattr(component, 'variants'):
            component_name = utils.component_name(component)
            print(f' - {component_name}')
            variants += component.variants()

    variants.sort(key=lambda v: v.name)
    yaml = utils.to_yaml(EvgProject(buildvariants=variants))
    utils.write_to_file(yaml, 'variants.yml')
