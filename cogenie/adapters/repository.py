import typing
from glob import glob

import requests
from consumer_generator.domain import model
from data_catalogue.domain.model import SchemaDefinition, SchemaFormat
from sidious.helpers import catalogue_url


class FileSystemConsumerRepository:
    def __init__(self, repository_path: str):
        self.repository_path = repository_path

    def add(self, consumer: model.Consumer):
        with open(consumer.file_path, "w") as f:
            f.write(consumer.payload)

    def get(self, file_path: str):
        with open(file_path, "r") as f:
            return f.read()

    def remove(self):
        raise NotImplementedError

    def list_all(self) -> typing.List[str]:
        rp = f"{self.repository_path}/*.py"
        return glob(rp)

    @property
    def schemas_from_catalogue(self, schema_format: SchemaFormat) -> SchemaDefinition:
        """
        TODO: This needs to be integration tested
        :param url:
            Endpoint url of data_catalogue/entrypoints/flask_app
        :param msf:
            See SchemaFormat in data_catalogue/domain/model
        :return:
            Schema definition in json. E.g see tests/schema/schema_one_v1.json
        """
        response = requests.get(catalogue_url(route="list_catalogue", refresh=True))
        response.raise_for_status()

        return response.json()
