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
    print(f"debug:{text}")

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
    try:
        if dataset == 'sales_target':
            if (element['month_of_order_date'] == '' or
                element['category'] == '' or
                element['target'] == ''
                ):
                yield pvalue.TaggedOutput('invalid_columns', element)
            else:
                yield pvalue.TaggedOutput('valid_columns', (str(element['month_of_order_date'])+str(element['category']), element))

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
    except Exception as e:
        print(f"ERROR:{element}")
        print(e)
        yield pvalue.TaggedOutput('invalid_columns', element)    

def merge_datasets(element:Tuple[str,Dict], sales_target:Dict):

    sales_target_df = pd.DataFrame(sales_target.values())

    # Convert date columns to period
    sales_target_df['sales_target_period'] = pd.to_datetime(sales_target_df['month_of_order_date'], format='%b-%y').dt.to_period('M').astype(str)

    order_id, elements = element

    list_of_orders = elements['A']
    order_details = elements['B']

    list_of_orders_df = pd.DataFrame(list_of_orders)
    order_details_df = pd.DataFrame(order_details)

    list_of_orders_df['order_date'] = pd.to_datetime(list_of_orders_df['order_date'], format='%d-%m-%Y')

    list_of_orders_df['order_period'] = list_of_orders_df['order_date'].dt.to_period('M').astype(str)

    list_of_orders_df['order_date'] = list_of_orders_df['order_date'].astype(str)

    orders_and_details_df = pd.merge(order_details_df, list_of_orders_df, how='left', left_on="order_id", right_on="order_id")

    orders_and_details_and_target_df = pd.merge(orders_and_details_df, sales_target_df, how='left', left_on=["order_period","category"], right_on=["sales_target_period","category"]).to_dict("records")

    for output in orders_and_details_and_target_df:
        yield output
