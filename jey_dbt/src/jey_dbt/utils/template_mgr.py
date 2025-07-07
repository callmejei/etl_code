import logging
from typing import Dict

from jinja2 import Environment, FileSystemLoader

from .constants import TEMPLATE_DIR

logger = logging.getLogger("cli_logger")

jinja = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    trim_blocks=True,
    lstrip_blocks=True
)


def generate_target_file(template_file: str, objects: Dict, target_file_path: str):
    """
    Function to generate target file.

    Args:
        template_file (str): The template file to be used for rendering
        objects (Dict): Objects to be rendered
        target_file_path (str): Target file path
    """
    template = jinja.get_template(template_file)
    generated_content = template.render(**objects)

    with open(target_file_path, "w") as fp:
        fp.write(generated_content)
