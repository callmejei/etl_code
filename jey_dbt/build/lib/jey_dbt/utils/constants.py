from enum import Enum
from pathlib import Path


class DBTProfileOptions(str, Enum):
    trino = "trino"
    hive = "hive"
    spark = "spark"


TEMPLATE_DIR: Path = Path(__file__).parent.parent.absolute() / "templates"

DBT_PROJECT_YAML_TEMPLATE = "dbt_project.yml.template"
DBT_PROJECT_YAML = "dbt_project.yml"
DBT_PACKAGES_YAML = "packages.yml"
DBT_PROFILE_FOLDER = "connection"
DBT_PROFILE_TEMPLATE = "profiles.yml.template"
DBT_PROFILE_YAML = "profiles.yml"

__CML_PROD_DOMAIN = ''
__CML_PREPROD_DOMAIN = ''
__CML_UAT_DOMAIN = ''
__CML_DR_DOMAIN = ''
