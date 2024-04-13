import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('glue')
glueJobName = "glue-redshift-join-job"

def lambda_handler(event, context):
    logger.info('Triggered by event: %s', event)
    
    try:
        response = client.start_job_run(JobName=glueJobName)
        logger.info('Started Glue job: %s', glueJobName)
        logger.info('Glue job run id: %s', response['JobRunId'])
        return response
    except Exception as e:
        logger.error('Error starting Glue job: %s', e)
        raise e
