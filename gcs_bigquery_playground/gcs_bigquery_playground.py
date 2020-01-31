import argparse
import logging
import time

import google.cloud.storage
import google.cloud.bigquery

LOGGER = logging.getLogger(__name__)


def _parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--service-key-path',
                        action='store',
                        help='service key json path')
    # BQ
    parser.add_argument('--bq-dataset', action='store', required=True)
    parser.add_argument('--bq-table', action='store', required=True)
    # GCS
    parser.add_argument('--gcs-bucket', action='store', required=True)
    # Local Content
    parser.add_argument('--data-folder', action='store', required=True)

    return parser.parse_args()


def _main():
    options = _parse_args()
    service_key_path = options.service_key_path
    data_folder = options.data_folder
    bq_dataset = options.bq_dataset
    bq_table = options.bq_table
    gsc_bucket = options.gcs_bucket

    # Clients
    gcs_storage = google.cloud.storage.Client()
    bq_client = google.cloud.bigquery.Client()

    # table_ref = self._dataset_ref.table(table_name)
    # table_schema = get_bigquery_serp_cache_table_schema()
    # table_obj = Table(table_ref, schema=table_schema)
    # job_config = LoadJobConfig()
    # job_config.schema = table_schema
    # return self._client.load_table_from_json(data, table_obj, job_config=job_config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
