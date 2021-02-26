#!/usr/bin/env python

"""A very simple what you publish is what you get Apache Beam streaming pipeline.
It reads JSON encoded messages from Pub/Sub and writes the results to BigQuery."""

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    ["SCHEMA_NAME:STRING", "SCHEMA_VERSION:INTEGER", "EVENT_TYPE_ID:INTEGER"]
)


def parse_json_message(message):
    row = json.loads(message)
    return {
        "SCHEMA_NAME": row["SCHEMA_NAME"],
        "SCHEMA_VERSION": row["SCHEMA_VERSION"],
        "EVENT_TYPE_ID": row["EVENT_TYPE_ID"],
    }


def run(args, input_subscription, output_table):
    """Build and run the pipeline."""
    options = PipelineOptions(args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:

        # Read the messages from PubSub and process them.
        messages = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA
        )


def _run():
    """
    Usage:
    simple_ingest
        --output_table "ancient-link-298622:ancient_link.schema_one_v1"
        --input_subscription projects/ancient-link-298622/subscriptions/default_sidious_subscription
    :return:
    """

    logging.getLogger().setLevel(logging.INFO)
    # TODO: maybe these could be hard_coded if this is code-generated
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Format: PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_subscription",
        help="Format: projects/<PROJECT>/subscriptions/<SUBSCRIPTION>",
    )
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args, known_args.input_subscription, known_args.output_table)
