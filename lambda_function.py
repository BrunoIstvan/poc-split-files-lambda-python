# import logging
from app.process import process

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    # verificar se os dados do arquivo vieram no evento
    if 'Records' in event and 's3' in event['Records'][0]:
            
        # recuperar o nome do bucket
        bucket = event['Records'][0]['s3']['bucket']['name']
        # recuperar o nome do arquivo
        key = event['Records'][0]['s3']['object']['key']
         
        print('>>> Process')  
        # logger.debug('>>> Process')  
        process(bucket=bucket, key=key)
        print('>>> Finish Process')  
        
    return {'response': 'OK'}
    
    
