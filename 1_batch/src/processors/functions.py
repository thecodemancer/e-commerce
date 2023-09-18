import logging
import time
from typing import Dict, List, Tuple, Any

import apache_beam as beam
import apache_beam.pvalue as pvalue

log = logging.getLogger(__name__)
log.setLevel(level=logging.INFO)

def debug(text):
    log.info(f"debug:{text}")

def filter(dataset, element){
    if dataset=='sales_target':
        pass
    if dataset=='list_of_orders':
        pass
    if dataset=='order_details':
        pass
}
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
