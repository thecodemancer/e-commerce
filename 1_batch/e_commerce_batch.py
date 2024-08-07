import json
import argparse
import datetime
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from src.processors.functions import *

log = logging.getLogger(__name__)
log.setLevel(level=logging.DEBUG)


def parse_profile_update_args(argv=None):
    '''
    job_name and template_location are provided by Dataflow at runtime. These are inputs for calculating the job_id
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--compute_project_id", dest="compute_project_id")
    parser.add_argument("--input_gcs", dest="input_gcs")
    parser.add_argument("--output_dataset", dest="output_dataset")
    parser.add_argument("--output_table", dest="output_table")

    known_args, pipeline_args = parser.parse_known_args(argv)
    return known_args, pipeline_args

def main(argv=None):
    pipeline_options = PipelineOptions(
        streaming=False,
        save_main_session=True
    )
    known_args, pipeline_args = parse_profile_update_args(argv)

    PROJECT_ID=known_args.compute_project_id    #'thecodemancer-e-commerce-12345'
    OUTPUT_DATASET=known_args.output_dataset #'E_Commerce'
    INPUT_GCS=known_args.input_gcs
    OUTPUT_TABLE=known_args.output_table
    LAST_UPDATE_DATE=datetime.datetime.now() #QA
    LAST_UPDATE_DATE_ISO=datetime.datetime.now().isoformat()#QA

    log.info("-"*200)
    log.info("Arguments ")
    log.info(f"PROJECT_ID:{PROJECT_ID}")
    log.info(f"INPUT_GCS:{INPUT_GCS}")
    log.info(f"OUTPUT_DATASET:{OUTPUT_DATASET}")
    log.info(f"OUTPUT_TABLE:{OUTPUT_TABLE}")
    log.info(f"LAST_UPDATE_DATE:{LAST_UPDATE_DATE}")
    log.info("-"*200)

    with beam.Pipeline(options=pipeline_options) as pipeline:
       
        sales_target = (
                pipeline | 'read sales_target' >> beam.io.ReadFromText(f'gs://{INPUT_GCS}/sales_target.csv', skip_header_lines=1)
                | 'filter valid rows in sales_target' >> beam.FlatMap(valid_rows, dataset='sales_target').with_outputs(
                                                                            'valid_rows',
                                                                            'invalid_rows'
                                                                            )   
        )
        sales_target2 = ( sales_target['valid_rows']
                | 'split sales_target' >> beam.Map(lambda line: line.split(','))
                | beam.Map(lambda line: sales_target_parse(line))
                | 'filter valid columns in sales_target' >> beam.FlatMap(valid_columns, dataset='sales_target').with_outputs(
                                                                            'valid_columns',
                                                                            'invalid_columns'
                                                                            )   
                )

        list_of_orders = (
                pipeline | 'read list_of_orders' >> beam.io.ReadFromText(f'gs://{INPUT_GCS}/list_of_orders.csv', skip_header_lines=1)
                | 'filter valid rows in list_of_orders' >> beam.FlatMap(valid_rows, dataset='sales_target').with_outputs(
                                                                            'valid_rows',
                                                                            'invalid_rows'
                                                                            )  
                )
        list_of_orders2 = ( list_of_orders['valid_rows']        
                | 'split list_of_orders' >> beam.Map(lambda line: line.split(','))
                | beam.Map(lambda line: list_of_orders_parse(line))
                | 'split list_of_orders by null values' >> beam.FlatMap(valid_columns, dataset='list_of_orders').with_outputs(
                                                                            'valid_columns',
                                                                            'invalid_columns'
                                                                            )
                )

        order_details = (
                pipeline | 'read order_details' >> beam.io.ReadFromText(f'gs://{INPUT_GCS}/order_details.csv', skip_header_lines=1)
                | 'filter valid rows in order_details' >> beam.FlatMap(valid_rows, dataset='order_details').with_outputs(
                                                                                            'valid_rows',
                                                                                            'invalid_rows'
                                                                                            )  
        )
        order_details2 = ( order_details['valid_rows']        
                | 'split order_details' >> beam.Map(lambda line: line.split(','))
                | beam.Map(lambda line: order_details_parse(line))
                | 'split order_details by null values' >> beam.FlatMap(valid_columns, dataset='order_details').with_outputs(
                                                                            'valid_columns',
                                                                            'invalid_columns'
                                                                            )
                )

        orders_and_details_and_target = (
            {"A":list_of_orders2['valid_columns'], "B": order_details2['valid_columns']}
            | "CoGroupByKey1" >> beam.CoGroupByKey()
            | beam.ParDo(merge_datasets, beam.pvalue.AsDict(sales_target2['valid_columns']))
        )

        # Write the rows to BigQuery.
        rows = ( orders_and_details_and_target 
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                project=f"{PROJECT_ID}",
                dataset=f"{OUTPUT_DATASET}",
                table=f"{OUTPUT_TABLE}",
                schema='order_id:STRING,amount:Float,profit:Float,quantity:INTEGER,category:STRING,sub_category:STRING,order_date:DATE,customer_name:STRING,state:STRING,city:STRING,order_period:STRING,month_of_order_date:STRING,target:FLOAT,sales_target_period:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://{INPUT_GCS}"
                )        
            )
               
#        beam.io.WriteToBigQuery(
#            # The table name is a required argument for the BigQuery sink.
#            # In this case we use the value passed in from the command line.
#            table=OUTPUT_TABLE, dataset=OUTPUT_DATASET, project=PROJECT_ID,
#            # JSON schema created from make_sink_schema function
#            schema=parse_table_schema_from_json(my_schema),
#            
#            # Creates the table in BigQuery if it does not yet exist.
#            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#            
#            # Deletes all data in the BigQuery table before writing.
#            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
#            )
            
if __name__ == "__main__":
    main()