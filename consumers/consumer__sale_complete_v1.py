#!/usr/bin/env python

import json
import logging
import typing
from datetime import date

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import (
    _BeamArgumentParser as BeamArgumentParser,
)

options = PipelineOptions(
    runner="DirectRunner",
    streaming=True,
    job_name="ingest-sale-complete-v1-to-bigquery",
    experiments="use_beam_bq_sink"
)


class ExtraOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):  # type: (BeamArgumentParser) -> None
        parser.add_argument("--subscription")
        parser.add_argument("--dataset")
        parser.add_argument("--table")


# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(['event_timestamp:TIMESTAMP', 'event_name:STRING', 'event_version:INTEGER', 'user_id:STRING', 'transaction_id:STRING', 'column_1:FLOAT', 'column_2:TIMESTAMP', 'column_3:BOOLEAN', 'date:DATE'])


def parse_json_message(message: str) -> typing.Dict[str, typing.Any]:

    row = json.loads(message)
    return {
            "event_timestamp": row["event_timestamp"],
            "event_name": row["event_name"],
            "event_version": row["event_version"],
            "user_id": row["user_id"],
            "transaction_id": row["transaction_id"],
            "column_1": row["column_1"],
            "column_2": row["column_2"],
            "column_3": row["column_3"],
            "date": row["date"],
    }


class IngestOrDiscard(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, row: typing.Dict[str, typing.Any]):
        """
        :param row:
        For now, we assume that every event has
        event_timestamp, event_name, event_version and date fields
        :return:
        """
        event_timestamp = row["event_timestamp"]
        if isinstance(event_timestamp, float) is False:
            logging.warning(f"{event_timestamp=} is not a FLOAT. Row discarded.")
            return

        event_name = row["event_name"]
        if isinstance(event_name, str) is False:
            logging.warning(f"{event_name=} is not a STRING. Row discarded.")
            return

        event_version = row["event_version"]
        if isinstance(event_version, int) is False or event_version < 1:
            logging.warning(f"{event_version=} is incorrect. Row discarded.")
            return

        partition_date = date.fromisoformat(row["date"])
        event_date = date.fromtimestamp(row["event_timestamp"])

        if partition_date != event_date:
            logging.warning(f"{partition_date=} and {event_date=} are different. Row discarded.")
            return

        return [row]


def run(subscription, dataset, table):
    """ to test locally change the runner="DataflowRunner" and execute the below on the command line
    python -m consumers.consumer__sale_complete_v1 \
      --project=heuristic-lumiere-2021 \
      --subscription=projects/heuristic-lumiere-2021/subscriptions/sale_complete_subscription \
      --dataset=pdm_data_lake \
      --table=sale_complete_v1
    """
    with beam.Pipeline(options=options) as p:

        messages = (
            p
            | "Read subscription" >> beam.io.ReadFromPubSub(subscription=subscription)
            | "UTF-8 bytes to string" >> beam.Map(lambda m: m.decode("utf-8"))
            | "Parse json" >> beam.Map(parse_json_message)
            | "Mini quality check" >> beam.ParDo(IngestOrDiscard())
        )

        table_id = f'{options.display_data()["project"]}:{dataset}.{table}'
        messages | beam.io.WriteToBigQuery(
            table=table_id,
            schema=SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    eo = ExtraOptions()
    run(eo.subscription, eo.dataset, eo.table)
