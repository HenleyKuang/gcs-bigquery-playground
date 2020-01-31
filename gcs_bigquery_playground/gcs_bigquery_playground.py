import argparse
import logging
import os
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
    parser.add_argument('--bq-project', action='store', required=True)
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
    bq_project = options.bq_project
    bq_dataset = options.bq_dataset
    bq_table = options.bq_table
    gcs_bucket = options.gcs_bucket

    # Variables
    current_time = int(time.time())

    # Clients
    storage_client = google.cloud.storage.Client()
    bq_client = google.cloud.bigquery.Client(project=bq_project)

    # Upload file
    bucket = storage_client.get_bucket(gcs_bucket)
    for f in os.listdir(data_folder):
        source_file_name = os.path.join(data_folder, f)
        blob = bucket.blob('%s/%s' % (current_time, f))
        blob.upload_from_filename(source_file_name)

    # Load Job to BQ
    dataset_ref = bq_client.dataset(bq_dataset)
    job_config = google.cloud.bigquery.LoadJobConfig()
    job_config.schema = [
        google.cloud.bigquery.SchemaField("name", "STRING", "REQUIRED"),
        google.cloud.bigquery.SchemaField("age", "INTEGER", "REQUIRED"),
    ]
    job_config.source_format = google.cloud.bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    uri = "gs://%(gcs_bucket)s/%(object_dir)s/*" % {
        'gcs_bucket': gcs_bucket,
        'object_dir': current_time,
    }
    table_ref = dataset_ref.table(bq_table)
    result_obj = bq_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )
    sleep_time = 1
    while result_obj.done() is False:
        LOGGER.info('waiting for %s second. data insertion.', sleep_time)
        time.sleep(sleep_time)

    if result_obj.errors:
        error_msg = 'Failed to insert: error_msg=%s' % result_obj.errors
        LOGGER.error(error_msg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _main()
