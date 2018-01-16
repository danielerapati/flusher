import csv

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig


DATASET_NAME = 'g_sheets'

client = bigquery.Client().from_service_account_json('service-account-key.json')

dataset = client.dataset(DATASET_NAME)

if not DATASET_NAME in [l.dataset_id for l in client.list_datasets()]:
    dataset = client.create_dataset(bigquery.Dataset(dataset))


def load_from_file(file_path, table):
    table_ref = dataset.table(table)
    load_config = LoadJobConfig()
    load_config.autodetect = True

    with open(file_path, 'rb') as readable:
        result = client.load_table_from_file(readable, table_ref, job_config=load_config)

    # return result
    return table_ref.dataset_id + '.' + table_ref.table_id


def table_exists(table_name):
    try:
        return client.get_table(dataset.table(table_name))
    except NotFound as e:
        return False


def delete(table_name):
    return client.delete_table(dataset.table(table_name))


def load(file_path, table, incremental=False):
    # TODO: return something more informative! how many rows loaded, how many new, the table name, etc.
    # TODO: some safety around deleting tables
    if not incremental and table_exists(table):
        delete(table)
    return load_from_file(file_path, table)
