from datetime import date
from os import environ, remove
from app.s3_service import put_object

# BUCKET_GLOBAL = environ['BUCKET_GLOBAL']
BUCKET_GLOBAL_BACKUP = environ['BUCKET_GLOBAL_BACKUP']
BUCKET_PENDING_PROCESS = environ['BUCKET_PENDING_PROCESS']


def process_file(blocks, prefix):

    for block in blocks:  # ignore the first line

        if block == '':
            continue

        cutted_block = cut_header_van(block, 26)
        current_establishment = cutted_block[50:55]

        # create a temporary file
        file_name = '/tmp/TEMP_FILE'
        building_file = open(file_name, mode='wb')

        building_file.write(cutted_block.encode())
        building_file.close()
        send_file = open(file_name, 'rb')
        today = date.today()
        est = current_establishment
        year = str(today.year).zfill(4)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)
        final_file_name = f'{prefix}-{year}-{month}-{day}-{est}.TXT'
        put_object(BUCKET_PENDING_PROCESS, final_file_name, send_file)  # OK
        send_file.close()
        remove(file_name)


def cut_header_van(block, size_to_cut):

    return block[size_to_cut:]
