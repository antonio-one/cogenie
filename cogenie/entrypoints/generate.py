"""
query the data_catalogue
get a list of schemas
if there's a new schema
create a new consumer
create a new test for that consumer
create a new git branch
do a pull request
The rest is a git/CI operational piece
"""

from os import listdir

import click

from cogenie.service_layer import consumer_generator
from cogenie.settings import CONSUMER_REPOSITORY_PATH, CONSUMER_TEMPLATE_PATH


@click.command()
def main():
    cg = consumer_generator.ConsumerGenerator(
        CONSUMER_REPOSITORY_PATH, CONSUMER_TEMPLATE_PATH
    )
    cg.generate()
    print(f"Consumers re-generated in: cogenie/{CONSUMER_REPOSITORY_PATH}/")
    print(listdir(CONSUMER_REPOSITORY_PATH))


if __name__ == "__main__":
    main()
