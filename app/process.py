from os import environ

from app.process_file import process_credit_sales_file, process_file
from app.process_debit_sales_file import process_debit_sales_file
from app.process_financial_file import process_financial_file
from app.process_outstanding_balance_file import process_outstanding_balance_file
from app.s3_service import get_object, move_object

# import logging

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)


BUCKET_GLOBAL = environ['BUCKET_GLOBAL']
BUCKET_GLOBAL_BACKUP = environ['BUCKET_GLOBAL_BACKUP']
BUCKET_PENDING_PROCESS = environ['BUCKET_PENDING_PROCESS']


def process(bucket, key):

    # logger.debug('>>> Get Data File')  
    print('>>> Get Data File')  
    # read all data from this file
    blocks = get_data_file(bucket=bucket, key=key)

    blocks = blocks[1:]

    # logger.debug('>>> Validate First Line')  
    print('>>> Validate First Line')  
    # validate first line after recover file type
    file_type = get_file_type(key)

    # logger.debug('>>> Run Process Credit Sales File')
    print('>>> Run Process Credit Sales File')
    process_file(blocks=blocks,
                 prefix=file_type)

    # logger.debug('>>> Run Move File to Backup Bucket')  
    print('>>> Run Move File to Backup Bucket')  
    move_object(bucket_origin=BUCKET_GLOBAL, 
                key_origin=key,
                bucket_destination=BUCKET_GLOBAL_BACKUP, 
                key_destination=key)


def get_data_file(bucket, key):
    
    file = get_object(bucket, key)
    if file is None:
        raise Exception('None file to be processed')
    file = file['Body']
    file = file.read().decode('utf8')
    blocks = file.split('@MAILBOX')
    return blocks


def get_file_type(key):

    file_type = key[0:4]

    if file_type not in ['EEVC', 'EEVD', 'EEFI', 'EESA']:
        raise Exception('File type not treated in this process')

    return file_type

