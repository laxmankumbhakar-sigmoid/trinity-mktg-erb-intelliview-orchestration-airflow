import argparse
import logging
import os
import platform
import shutil
import sys
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


class EnumAction(argparse.Action):
    """
    Argparse action for handling Enums
    """

    def __init__(self, **kwargs):
        # Pop off the type value
        enum = kwargs.pop("type", None)

        # Ensure an Enum subclass is provided
        if enum is None:
            raise ValueError("type must be assigned an Enum when using EnumAction")
        if not issubclass(enum, Enum):
            raise TypeError("type must be an Enum when using EnumAction")

        # Generate choices from the Enum
        kwargs.setdefault("choices", tuple(e.value for e in enum))

        super(EnumAction, self).__init__(**kwargs)

        self._enum = enum

    def __call__(self, parser, namespace, values, option_string=None):
        # Convert value back into an Enum
        enum = self._enum(values)
        setattr(namespace, self.dest, enum)


class Environment(Enum):
    dev = "dev"
    prod = "prod"


DAG_BUCKETS = {"dev": "europe-west2-intelliv2-sbx--ce371c98-bucket", "prod": ""}


def generate_dags_deploy_command(env, write_path="builds/dags"):
    logging.info("Generating DAG deploy script")
    operating_system = platform.system()

    script_name = "deploy.ps1" if operating_system == "Windows" else "deploy.sh"
    command_lines = ["# Local build paths"]

    if operating_system == "Windows":
        command_lines.append("""$DAGS_BUILDS_PATH="$pwd"\builds\dags""")
    else:
        command_lines.append("""DAGS_BUILDS_PATH=`pwd`/builds/dags""")
    command_lines.append("# GCS build paths")

    if operating_system == "Windows":
        command_lines.append(
            f"""$DAGS_CLOUD_BASE_PATH="gs://{DAG_BUCKETS[env]}" """
        )
    else:
        command_lines.append(f"""DAGS_CLOUD_BASE_PATH=gs://{DAG_BUCKETS[env]}/dags""")

    command_lines.extend(
        [
            "gcloud storage cp -r $DAGS_BUILDS_PATH/** $DAGS_CLOUD_BASE_PATH",
            "gcloud storage rm $DAGS_CLOUD_BASE_PATH/deploy.sh",
        ]
    )

    with open(os.path.join(write_path, script_name), "w") as f:
        f.writelines([line.strip() + "\n" for line in command_lines])


def get_inputs(*args):
    parser = argparse.ArgumentParser(
        description="Build artifacts for supplied environment"
    )
    parser.add_argument(
        "env",
        metavar="env",
        type=Environment,
        action=EnumAction,
        help="Environment name",
    )
    parser.add_argument("--source", type=str, help="Source to build artifacts for")
    parser.add_argument(
        "--include_trinity",
        action="store_true",
        help="Whether to include Trinity artifacts in the build",
    )
    parsed_args = parser.parse_args()
    return vars(parsed_args)


if __name__ == "__main__":
    system_variables = get_inputs(sys.argv[1:])
    choice = system_variables["env"]
    source = system_variables["source"]
    trinity = system_variables["include_trinity"]

    # PATHS
    builds_path = "builds"
    dags_folder = "dags"

    if os.path.exists(builds_path) and os.path.isdir(builds_path):
        logging.info(
            "Found builds directory in current path. Deleting existing builds directory"
        )
        shutil.rmtree(builds_path)

    logging.info("Creating a new builds directory")
    os.mkdir(builds_path)

    logging.info(f"Creating a {dags_folder} folder to place dag builds")

    os.mkdir(os.path.join(builds_path, dags_folder))
    logging.info(f"Attempting to build DAGs for {choice.value}")

    def ignore_files_and_dirs(dir, contents):
        return [f for f in contents if f in ["__pycache__", ".airflowignore"]]

    def ignore_files_and_dirs_with_trinity(dir, contents):
        return [
            f for f in contents if f in ["__pycache__", ".airflowignore", "trinity"]
        ]

    if source:
        shutil.copytree(
            f"dags/{source}/",
            os.path.join(builds_path, dags_folder, source),
            ignore=ignore_files_and_dirs,
            dirs_exist_ok=True,
        )
    else:
        shutil.copytree(
            f"dags/",
            os.path.join(builds_path, dags_folder),
            ignore=ignore_files_and_dirs_with_trinity,
            dirs_exist_ok=True,
        )

    if trinity:
        shutil.copytree(
            "dags/trinity/",
            os.path.join(builds_path, dags_folder, "trinity"),
            ignore=ignore_files_and_dirs,
            dirs_exist_ok=True,
        )

    logging.info(f"All DAGs for {choice.value} built successfully")

    logging.info("Generating deploy scripts")
    generate_dags_deploy_command(choice.name, f"{builds_path}/{dags_folder}")
