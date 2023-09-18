import logging
import time
from typing import Dict, List, Tuple, Any

import apache_beam as beam
import apache_beam.pvalue as pvalue

log = logging.getLogger(__name__)
log.setLevel(level=logging.INFO)

def debug(text):
    log.info(f"debug:{text}")


# Define a function to parse the CSV rows into a dictionary format
def list_of_orders_parse(row):
    return {
        'order_id': row[0],
        'order_date': row[1],
        'customer_name': row[2],
        'state': row[3],
        'city': row[4]
    }

def sales_target_parse(row):
    return {
        'month_of_order_date': row[0],
        'category': row[1],
        'target': float(row[2])
    }

def order_details_parse(row):
    return {
        'order_id': row[0],
        'amount': float(row[1]),
        'profit': float(row[2]),
        'quantity': int(row[3]),
        'category': row[4],
        'sub_category': row[5]
    }

def filter_rows(dataset, element):
    if dataset=='sales_target':
        if (len(element['month_of_order_date']) == 0 or 
            len(element['category']) == 0 or
            len(element['target']) == 0
            ) :
            yield pvalue.TaggedOutput('sales_target_null', element)
        else:
            yield pvalue.TaggedOutput('sales_target_not_null', element)
    if dataset=='list_of_orders':
        if (len(element['order_id']) == 0 or 
            len(element['order_date']) == 0 or
            len(element['customer_name']) == 0 or
            len(element['state']) == 0 or
            len(element['city']) == 0
            ) :
            yield pvalue.TaggedOutput('list_of_orders_null', element)
        else:
            yield pvalue.TaggedOutput('list_of_orders_not_null', element)

    if dataset=='order_details':
        if (len(element['order_id']) == 0 or
            len(element['amount']) == 0 or 
            len(element['profit']) == 0 or
            len(element['quantity']) == 0 or
            len(element['category']) == 0 or
            len(element['sub_category']) == 0
            ) :
            yield pvalue.TaggedOutput('order_details_null', element)
        else:
            yield pvalue.TaggedOutput('order_details_not_null', element)

def split_talent_collection(element):
    for i, experience in enumerate(element['profiles'][0]['experience']):
        if experience.get('title'):
            yield pvalue.TaggedOutput('experiences', (experience['title'].lower(), {'coreSignalId':element['coreSignalId'], "position": i}))

    for j, current in enumerate(element['current']):  
        if current.get('title'):
            yield pvalue.TaggedOutput('current', (current['title'].lower(), {'coreSignalId':element['coreSignalId'], "position": j}))

    if element['profiles'][0].get('occupation'):
        yield pvalue.TaggedOutput('main_title', (element['profiles'][0]['occupation'].lower(), {'coreSignalId':element['coreSignalId'], "position": 0}))

def divide(element:Tuple[str,Dict,Dict]):
    yield pvalue.TaggedOutput('mongodb', element)
    yield pvalue.TaggedOutput('elasticsearch',element)
