import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Orders')

def lambda_handler(event, context):
    for record in event['Records']:
        # Extract message from SQS -> SNS wrapper
        sns_message = json.loads(record['body'])
        message = json.loads(sns_message['Message'])  # Unwrap SNS payload
        
        # Validate required fields
        if 'orderId' not in message:
            raise ValueError("Missing 'orderId' in message")
            
        # Save to DynamoDB
        table.put_item(Item=message)
        print(f"Order {message['orderId']} saved to DynamoDB")
    return {'statusCode': 200}