import typing
from pathlib import Path
from string import Template
from urllib.parse import urlunsplit

import requests

from cogenie.adapters import repository
from cogenie.domain import model
from cogenie.settings import (
    CONSUMER_REPOSITORY_PATH,
    DATCAT_HOST,
    DATCAT_PORT,
    DATCAT_SCHEME, DATASET_ID
)

DATCAT_NETLOC = f"{DATCAT_HOST}:{DATCAT_PORT}"
SCHEMA_URL_COMPONENTS = (DATCAT_SCHEME, DATCAT_NETLOC, "v1/datcat/schemas/list/refresh/true", "", "")
SCHEMA_URL = urlunsplit(SCHEMA_URL_COMPONENTS)


class ConsumerGenerator:
    def __init__(
        self,
        consumer_repository: repository.FileSystemConsumerRepository,
        template_path: str,
    ):
        self.consumer_repository = consumer_repository
        self.template_path = template_path

    @property
    def template(self) -> Template:
        with open(self.template_path, "r") as t:
            return Template(t.read())

    def job_name(self, schema_key: str) -> str:
        converted_key = schema_key.replace("_", "-")
        return f"ingest-{converted_key}-to-bigquery"

    @property
    def _imports(self):
        return NotImplementedError

    def _field_list(self, schema: typing.List[typing.Dict[str, str]]) -> str:
        # construct a field list of string like ["name:type",]
        field_list: typing.List[str] = []
        for field in schema:
            beam_field = ":".join([field["name"], field["type"]])
            field_list.append(beam_field)
        return field_list

    def _simple_parse(
        self, schema: typing.List[typing.Dict[str, typing.Any]]
    ) -> typing.Dict:
        output: str = ""
        for field in schema:
            output += f'"{field["name"]}": row["{field["name"]}"],' f"\n" f'{" "*12}'
        return output[:-13]

    def _data_quality_parse(self):
        # TODO: Add some basic datatype and null checks as a new function
        return NotImplementedError

    def _runner(self):
        # TODO: Read a little more about beam
        return NotImplementedError

    def generate(self) -> str:
        response = requests.get(url=SCHEMA_URL)
        schema_catalogue: typing.Dict = response.json()
        for schema_key, schema_value in schema_catalogue.items():
            consumer = model.Consumer()
            consumer.job_name = self.job_name(schema_key)
            consumer.file_path = (
                Path(CONSUMER_REPOSITORY_PATH) / f"consumer__{schema_key}.py"
            )
            consumer.field_list = self._field_list(schema=schema_value)
            consumer.row_parser = self._simple_parse(schema=schema_value)

            if bool(consumer) is False:
                raise ValueError("Not all consumer class properties have been set")

            consumer.payload = self.template.substitute(
                job_name=self.job_name(schema_key),
                field_list=self._field_list(schema=schema_value),
                simple_parse=self._simple_parse(schema=schema_value),
                cmd_consumer_filename=f"consumer__{schema_key}",
                cmd_project_id=DATASET_ID.partition(".")[0],
                cmd_subscription_name=f"{schema_key.rpartition('_')[0]}_subscription",
                cmd_dataset_name=DATASET_ID.rpartition(".")[-1],
                cmd_table_name=schema_key
            )

            consumer_repository = repository.FileSystemConsumerRepository(
                repository_path=CONSUMER_REPOSITORY_PATH
            )
            consumer_repository.add(consumer)
