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

from consumer_generator.service_layer import consumer_generator
from settings import CONSUMER_REPOSITORY_PATH, CONSUMER_TEMPLATE_PATH


def _run():
    cg = consumer_generator.ConsumerGenerator(
        CONSUMER_REPOSITORY_PATH, CONSUMER_TEMPLATE_PATH
    )
    cg.generate()
    print(listdir(CONSUMER_REPOSITORY_PATH))
