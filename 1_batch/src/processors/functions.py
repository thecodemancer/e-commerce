import logging
import time
import pandas as pd
from typing import Dict, List, Tuple, Any

import apache_beam as beam
import apache_beam.pvalue as pvalue

log = logging.getLogger(__name__)
log.setLevel(level=logging.INFO)

def debug(text:str):
    log.info(f"debug:{text}")


# Define a function to parse the CSV rows into a dictionary format
def list_of_orders_parse(row:str):
    return {
        'order_id': row[0],
        'order_date': row[1],
        'customer_name': row[2],
        'state': row[3],
        'city': row[4]
    }

def sales_target_parse(row:str):
    return {
        'month_of_order_date': row[0],
        'category': row[1],
        'target': float(row[2])
    }

def order_details_parse(row:str):
    return {
        'order_id': row[0],
        'amount': float(row[1]),
        'profit': float(row[2]),
        'quantity': int(row[3]),
        'category': row[4],
        'sub_category': row[5]
    }

def valid_rows(element, dataset: str):
  if element.strip() == '':
    yield pvalue.TaggedOutput('invalid_rows', (dataset, element))
  else:
    yield pvalue.TaggedOutput('valid_rows', element)

def valid_columns(element: Dict, dataset: str):
#  try:
    if dataset == 'sales_target':
        if (element['month_of_order_date'] == '' or
            element['category'] == '' or
            element['target'] == ''
            ):
          yield pvalue.TaggedOutput('invalid_columns', element)
        else:
          yield pvalue.TaggedOutput('valid_columns', (element['month_of_order_date'], element))

    if dataset == 'list_of_orders':
        if (
            element['order_id'] == '' or
            element['order_date'] == '' or
            element['customer_name'] == '' or
            element['state'] == '' or
            element['city'] == ''
            ):
            yield pvalue.TaggedOutput('invalid_columns', element)
        else:
            yield pvalue.TaggedOutput('valid_columns', (element['order_id'], element))

    if dataset == 'order_details':
        if (element['order_id'] == '' or
            element['amount'] == '' or
            element['profit'] == '' or
            element['quantity'] == '' or
            element['category'] == '' or
            element['sub_category'] == ''
            ):
            yield pvalue.TaggedOutput('invalid_columns', element)
        else:
            yield pvalue.TaggedOutput('valid_columns', (element['order_id'], element))
#  except Exception as e:
#    print(f"ERROR:{element}")
#    print(e)

def merge_datasets(element):
  for e1 in element[1]['A']:
    df1=pd.DataFrame([e1])

    # Convert date columns to a common format
#    df1['order_period'] = pd.to_datetime(df1['order_date'], format='%d-%m-%Y').dt.to_period('M')

    for e2 in element[1]['B']:
      df2=pd.DataFrame([e2])
      df3=df1.merge(df2, how='left', left_on="order_id", right_on="order_id").to_dict("records")[0]
      yield df3
#(element['month_of_order_date'], element)
def merge_datasets2(element):
    return element