from os import environ

from app.process_credit_sales_file import process_credit_sales_file
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
    lines = get_data_file(bucket=bucket, key=key)

    # logger.debug('>>> Validate First Line')  
    print('>>> Validate First Line')  
    # validate first line after recover file type
    file_type, line = validate_first_line(lines)

    if file_type in ['EEVC', 'NNVC', 'NEVC']:
        # logger.debug('>>> Run Process Credit Sales File')  
        print('>>> Run Process Credit Sales File')  
        process_credit_sales_file(header=line, 
                                  lines=lines[1:], 
                                  prefix='EEVC')
                                  
    elif file_type in ['EEVD', 'NNVD', 'NEVD']:
        # logger.debug('>>> Run Process Debit Sales File')  
        print('>>> Run Process Debit Sales File')  
        process_debit_sales_file(header=line, 
                                 lines=lines[1:], 
                                 prefix='EEVD')
                                 
    elif file_type in ['EEFI', 'NNFI', 'NEFI']:
        # logger.debug('>>> Run Process Financial File')  
        print('>>> Run Process Financial File')  
        process_financial_file(header=line, 
                               lines=lines[1:], 
                               prefix='EEFI')
                               
    elif file_type in ['EESA', 'NNSA', 'NESA']:
        # logger.debug('>>> Run Process Outstanding Balance File')  
        print('>>> Run Process Outstanding Balance File')  
        process_outstanding_balance_file(header=line, 
                                         lines=lines[1:], 
                                         prefix='EESA')

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
    return file.split('\n')


def validate_first_line(lines):
    
    if lines[0][19:20] == '@':  # entao eh cabecalho com tipo de arquivo
        file_type = lines[0][48:52]

    if file_type not in ['EEVC', 'NNVC', 'NEVC',
                         'EEVD', 'NNVD', 'NEVD',
                         'EEFI', 'NNFI', 'NEFI',
                         'EESA', 'NNSA', 'NESA']:
        raise Exception('File type not treated in this process')

    return file_type, lines[0]

