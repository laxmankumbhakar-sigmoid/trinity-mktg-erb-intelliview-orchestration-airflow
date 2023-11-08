import json
import os

from airflow.models import Variable

ENVIRONMENT = Variable.get("env")
CONFIG_PATH = f"/home/airflow/gcs/data/environments/{ENVIRONMENT}.json"

if not os.path.exists(CONFIG_PATH):
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    CONFIG_PATH = os.path.join(parent_dir, "data", "environments", f"{ENVIRONMENT}.json")


with open(CONFIG_PATH, "r") as config_file:
    CONFIG = json.load(config_file)


class BlobStorage:
    __BLOBCONFIG = CONFIG["blob-storage"]

    class __CloudStorage:
        def __init__(self, name: str, type: str):
            self.__name = name
            self.__type = type

        @property
        def URI(self):
            if self.__type == "adls1":
                directory, account = self.__name.split("@")
                return f"adl://{account}.azuredatalakestore.net/{directory}"
            elif self.__type == "adls2":
                return f"abfss://{self.__name}.dfs.core.windows.net"
            elif self.__type == "gcs":
                return f"gs://{self.__name}"

        def path(self, *parts):
            paths = [part.strip("/") for part in parts]
            return "/".join([self.URI, *paths])

        @property
        def bucket(self):
            return self.__name

    for __key, __value in __BLOBCONFIG.items():
        __cloud_storage = __CloudStorage(__value["name"], __value["type"])
        locals()[__key] = __cloud_storage
