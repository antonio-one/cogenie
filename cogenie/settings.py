from decouple import config

CONSUMER_REPOSITORY_PATH = config("CONSUMER_REPOSITORY_PATH")
CONSUMER_TEMPLATE_PATH = config("CONSUMER_TEMPLATE_PATH")
DATCAT_SCHEME = config("DATCAT_SCHEME")
DATCAT_HOST = config("DATCAT_HOST")
DATCAT_PORT = config("DATCAT_PORT")
DATASET_ID = config("DATASET_ID")
GOOGLE_APPLICATION_CREDENTIALS = config("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID, DATASET_NAME = config("DATASET_ID").split(".")
LOCATION = config("LOCATION")
