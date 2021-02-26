import typing
from pathlib import Path
from string import Template

import requests
from consumer_generator.adapters import repository
from consumer_generator.domain import model
from settings import CONSUMER_REPOSITORY_PATH
from sidious.helpers import catalogue_url


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
        response = requests.get(url=catalogue_url(route="list_catalogue", refresh=True))
        schema_catalogue: typing.Dict = response.json()
        for schema_key, schema_value in schema_catalogue.items():
            consumer = model.Consumer()
            consumer.file_path = (
                Path(CONSUMER_REPOSITORY_PATH) / f"consumer__{schema_key}.py"
            )
            consumer.field_list = self._field_list(schema=schema_value)
            consumer.row_parser = self._simple_parse(schema=schema_value)

            if bool(consumer) is False:
                raise ValueError("Not all consumer class properties have been set")

            consumer.payload = self.template.substitute(
                field_list=self._field_list(schema=schema_value),
                simple_parse=self._simple_parse(schema=schema_value),
            )

            consumer_repository = repository.FileSystemConsumerRepository(
                repository_path=CONSUMER_REPOSITORY_PATH
            )
            consumer_repository.add(consumer)
