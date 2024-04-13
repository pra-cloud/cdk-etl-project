import json
import boto3

client = boto3.client('glue')

def lambda_handler(event, context):
    print('Crawlers starting')
    
    # Trigger the first crawler
    response1 = client.start_crawler(Name='lead-s3-crawler')
    print(json.dumps(response1))
    
    # Trigger the second crawler after the first one completes
    response2 = client.start_crawler(Name='activity-s3-crawler')
    print(json.dumps(response2))
    
    # Trigger the third crawler after the second one completes
    response3 = client.start_crawler(Name='task-s3-crawler')
    print(json.dumps(response3))

    # You can handle the response as needed or return it
    return {
        'statusCode': 200,
        'body': json.dumps('Crawlers triggered successfully')
    }
