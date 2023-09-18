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

    LAST_UPDATE_DATE=datetime.datetime.now() #QA
    LAST_UPDATE_DATE_ISO=datetime.datetime.now().isoformat()#QA

    log.info("-"*200)
    log.info("Arguments ")
    log.info(f"input_gcs:{known_args.input_gcs}")
    log.info(f"output_table:{known_args.output_table}")
    log.info(f"LAST_UPDATE_DATE:{LAST_UPDATE_DATE}")
    log.info("-"*200)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the CSV file
        sales_target = (
                pipeline | 'sales_target' >> beam.io.ReadFromText(f"gs://{input_gcs}/sales_target.csv", skip_header_lines=1)
                | beam.Map(lambda x: debug(x))
                )


        #list_of_orders = (
        #        pipeline | 'list_of_orders' >> beam.io.ReadFromText('gs://thecodemancer_e_commerce/list_of_orders.csv', skip_header_lines=1)
        #        | beam.Map(lambda x: log.info(x))
        #        )

        #order_details = (
        #        pipeline | 'order_details' >> beam.io.ReadFromText('gs://thecodemancer_e_commerce/order_details.csv', skip_header_lines=1)
        #        | beam.Map(lambda x: log.info(x))
        #        )


if __name__ == "__main__":
    main()
