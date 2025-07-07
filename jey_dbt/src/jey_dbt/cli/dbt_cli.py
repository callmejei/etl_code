import logging
import os
import shutil

import typer
from jey_dbt.utils import common
from rich.progress import Progress

from jey_dbt.utils.common import run_shell_command
from jey_dbt.utils.constants import (
    DBT_PACKAGES_YAML,
    DBT_PROFILE_FOLDER,
    DBT_PROFILE_TEMPLATE,
    DBT_PROFILE_YAML,
    DBT_PROJECT_YAML,
    DBT_PROJECT_YAML_TEMPLATE,
    TEMPLATE_DIR,
    DBTProfileOptions,
)

from jey_dbt.utils.template_mgr import generate_target_file

cli = typer.Typer()

logger = logging.getLogger("cli_logger")
cml_env = common.check_cml_env()


@cli.command()
def hello_dbt():
    """
    Methods to say hello for dbt cli
    """
    print("Hello! This is the Jey DBT CLI!")


@cli.command()
def init(
    project_name: str = typer.Argument(..., help="Name of your dbt project"),
    profile_type: DBTProfileOptions = typer.Option(
        DBTProfileOptions.trino,
        help="Which profile type to use.",
        case_sensitive=False,
      #  hidden=True,
    ),
    target_dir: str = typer.Option(
        ..., envvar="HOME", help="Target dir to put your project. Default to your $HOME"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        help="Whether to show progress message. Default to False. Enable it with '--verbose'",
    ),
):
    """
    Create a dbt project directory compliant with our requirements

    Args:
        project_name (str): Name of user's dbt project.
        profile_type (enum): Which profile type to use.
        target_dir (str): Target dir to put dbt project. Default to your $HOME
        verbose (bool): Whether to show progress message.
    """

    # TODO:
    # 1. db_project.yml to be generated using template and the content read from dbt-generated yaml
    # 2. package.yml directly copied over
    # 3. __init__.py to be generated based on template

    if project_name in os.listdir(path=target_dir):
        logger.warning("This project already exists!")
    else:
        # run dbt init
        if verbose:
            logger.info(f"Initialization of dbt project: {project_name}")

    total_steps = 7
    with Progress() as progress:
        try:
            init_task = progress.add_task("[cyan]Initializing...", total=total_steps)

            run_shell_command(
                f"cd {target_dir} && dbt init {project_name}  --skip-profile-setup"
            )

            progress.update(init_task, advance=1)
            if verbose:
                logger.info("Basic initialization has been done.")

            project_objects = {
                "project_name": project_name,
                "profile_type": profile_type.value,
            }

            # generate proper dbt_project.yml
            dbt_project_yaml_file = os.path.join(
                target_dir, project_name, DBT_PROJECT_YAML
            )
            generate_target_file(
                DBT_PROJECT_YAML_TEMPLATE, project_objects, dbt_project_yaml_file
            )
            progress.update(init_task, advance=1)
            if verbose:
                logger.info("dbt_project.yaml has been configured.")

            # cp packages.yml
            packages_yaml_target_file = os.path.join(
                target_dir, project_name, DBT_PACKAGES_YAML
            )
            packages_yaml_original_file = os.path.join(
                TEMPLATE_DIR, DBT_PACKAGES_YAML
            )
            shutil.copy(packages_yaml_original_file, packages_yaml_target_file)
            progress.update(init_task, advance=1)
            if verbose:
                logger.info("packages.yaml has been generated.")

            # generate essential folders
            os.makedirs(os.path.join(target_dir, project_name, DBT_PROFILE_FOLDER))
            os.makedirs(
                os.path.join(
                    target_dir, project_name, DBT_PROFILE_FOLDER, profile_type
                )
            )
            progress.update(init_task, advance=1)
            if verbose:
                logger.info(
                    f"{DBT_PROFILE_FOLDER}/{profile_type} folders have been created."
                )

            # generate profiles yaml
            dbt_profile_yaml_file = os.path.join(
                target_dir,
                project_name,
                DBT_PROFILE_FOLDER,
                profile_type,
                DBT_PROFILE_YAML,
            )
            generate_target_file(
                DBT_PROFILE_TEMPLATE, project_objects, dbt_profile_yaml_file
            )
            progress.update(init_task, advance=1)
            if verbose:
                logger.info(
                    f"profiles.yml has been created in {DBT_PROFILE_FOLDER}/{profile_type} "
                    + " you need to update the file"
                )

            # run dbt deps
            if cml_env != "NO_CML":
                run_shell_command(
                    f"cd {target_dir} && dbt deps --project-dir {project_name}"
                )
                progress.update(init_task, advance=1)
                if verbose:
                    logger.info("dbt deps has been configured.")

            logger.info("Initialization has been done!")

        except Exception as e:
            logger.error("Exception occurred while running the init command", exc_info=True)
            raise
