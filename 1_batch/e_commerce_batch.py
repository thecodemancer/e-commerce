import json
import argparse
import datetime
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from src.processors import (
    debug
)

log = logging.getLogger(__name__)
log.setLevel(level=logging.DEBUG)


def parse_profile_update_args(argv=None):
    '''
    job_name and template_location are provided by Dataflow at runtime. These are inputs for calculating the job_id
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_gcs", dest="input_gcs")
    parser.add_argument("--output_table", dest="output_table")

    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args

def main(argv=None):
    pipeline_options = PipelineOptions(
        streaming=False
    )
    known_args, pipeline_args = parse_profile_update_args(argv)

    INPUT_GCS=known_args.input_gcs
    OUTPUT_TABLE=known_args.output_table
    LAST_UPDATE_DATE=datetime.datetime.now() #QA
    LAST_UPDATE_DATE_ISO=datetime.datetime.now().isoformat()#QA

    log.info("-"*200)
    log.info("Arguments ")
    log.info(f"INPUT_GCS:{INPUT_GCS}")
    log.info(f"OUTPUT_TABLE:{OUTPUT_TABLE}")
    log.info(f"LAST_UPDATE_DATE:{LAST_UPDATE_DATE}")
    log.info("-"*200)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the CSV file
        sales_target = (
                pipeline | 'sales_target' >> beam.io.ReadFromText(f"gs://{INPUT_GCS}/sales_target.csv", skip_header_lines=1)
                | beam.Map(lambda x: debug(x))
        #        | beam.Map(filter_rows, dataset='sales_target').with_outputs(
        #                                                                    'sales_target_null',
        #                                                                    'sales_target_not_null'
        #                                                                    )
                )


        list_of_orders = (
                pipeline | 'list_of_orders' >> beam.io.ReadFromText('gs://thecodemancer_e_commerce/list_of_orders.csv', skip_header_lines=1)
                | beam.Map(lambda x: debug(x))
        #        | beam.Map(filter_rows, dataset='list_of_orders').with_outputs(
        #                                                                    'list_of_orders_null',
        #                                                                    'list_of_orders_not_null'
        #                                                                    )
        #
                )

        order_details = (
                pipeline | 'order_details' >> beam.io.ReadFromText('gs://thecodemancer_e_commerce/order_details.csv', skip_header_lines=1)
                | beam.Map(lambda x: debug(x))
        #        | beam.Map(filter_rows, dataset='order_details').with_outputs(
        #                                                                    'order_details_null',
        #                                                                    'order_details_not_null'
        #                                                                    )

                )

if __name__ == "__main__":
    main()
