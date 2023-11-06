import os

import yaml
from jinja2 import Environment, FileSystemLoader


def format_code(file):
    import subprocess

    try:
        import black
    except ImportError:
        # Install black
        subprocess.run(["pip", "install", "black"], check=True)

    command = f"black {file}"
    subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE)


file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))

template = env.get_template("template.jinja2")

config_dir = os.path.join(file_dir, "config")

for filename in os.listdir(config_dir):
    if filename.endswith(".yaml"):
        with open(f"{config_dir}/{filename}", "r") as configfile:
            config = yaml.safe_load(configfile)

            config["conf"] = {
                "data_interval_start": "{{ ds }}",
                "data_interval_end": "{{ ds }}",
            }

            for key in config.keys():
                if key.startswith("extraction"):
                    temp = "_".join(key.split("_")[1:])
                    config["conf"][temp] = config[key]

            file = f"dags/{config['data_product']}_{config['datasource_id']}/{config['data_product']}_{config['datasource_id']}_main.py"
            with open(file, "w") as f:
                f.write(template.render(config))

            format_code(file)
