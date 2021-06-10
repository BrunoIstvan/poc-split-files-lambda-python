from datetime import date
from os import environ, remove
from app.s3_service import put_object

# BUCKET_GLOBAL = environ['BUCKET_GLOBAL']
BUCKET_GLOBAL_BACKUP = environ['BUCKET_GLOBAL_BACKUP']
BUCKET_PENDING_PROCESS = environ['BUCKET_PENDING_PROCESS']


def process_outstanding_balance_file(header, lines, prefix):
    
    register_type = None
    file_type = None
    current_establishment = None

    # create a temporary file
    file_name = '/tmp/TEMP_FILE'
    building_file = open(file_name, mode='wb')
    # write header
    building_file.write(f'{header}\n'.encode())

    for line in lines:  # ignore the first line

        # get register type
        register_type = line[19:22]

        if '@MA' in register_type:
            
            file_name = '/tmp/TEMP_FILE'
            building_file = open(file_name, mode='wb')
            building_file.write(f'{header}\n'.encode())
            continue

        # if register_type is header of a headquarter
        elif register_type == '002':

            current_establishment = line[75:82]
            # file_name = 'TEMP_FILE'
            # building_file = open(file_name, mode='wb')
            building_file.write(f'{line[19:]}\n'.encode())
            continue

        # if register_type is different that footer, then continue writing in file
        elif not register_type == '028' and building_file is not None:

            building_file.write(f'{line[19:]}\n'.encode())
            continue

        # if register_type is the footer, save file, send to bucket and go next line
        elif register_type == '028' and building_file is not None:

            building_file.write(f'{line[19:]}\n'.encode())
            building_file.close()
            send_file = open(file_name, 'rb')
            today = date.today()
            est = current_establishment
            year = str(today.year).zfill(4)
            month = str(today.month).zfill(2)
            final_file_name = f'{est}/{year}/{month}/{prefix}.TXT'
            put_object(BUCKET_PENDING_PROCESS, final_file_name, send_file)  # OK
            send_file.close()
            remove(file_name)
            register_type = None
            file_name = None
            file_type = None
            current_establishment = None
            building_file = None
            continue
