import os
from pathlib import Path

from setuptools import find_namespace_packages, setup

# setup.py
setup(
    name="jey-dbt",
    version="0.1",
    python_requires=">=3.6",
    install_requires=["typer>=0.9.0", "dbt-spark==1.9.2", "dbt-trino==1.9.0"],
    packages=find_namespace_packages(where="src"),
    package_dir={"jey_dbt": "src/jey_dbt"},
    package_data={"jey_dbt.templates": ["*.template", "*.yml"]},
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": ["jey-dbt = jey_dbt.main:cli"]
    },
)
