import boto3

from moto import mock_aws
import main
import json
import unittest


class TestClass(unittest.TestCase):

    @mock_aws
    def test_get_widget_data_returnsdata(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=main.bucket_name)
        bucket = conn.Bucket(main.bucket_name)
        test_widget_filename = "test-widget.json"
        bucket.upload_file(test_widget_filename, str(1))
        widget_data = dict()
        with open(test_widget_filename, 'r') as file:
            widget_data = json.load(file)
        data = main.get_widget_data()
        for key in widget_data:
            assert key in data
            assert data[key] == widget_data[key]

    @mock_aws
    def test_process_widget_correctlycreatesdynamodata(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=main.bucket_name)
        bucket = conn.Bucket(main.bucket_name)
        test_widget_filename = "test-widget.json"
        bucket.upload_file(test_widget_filename, str(1))
        widget_data = dict()
        with open(test_widget_filename, 'r') as file:
            widget_data = json.load(file)
        dynamodb = boto3.resource('dynamodb')
        dynamodb.create_table(
            TableName=main.table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table = dynamodb.Table(main.table_name)
        main.process_widget(widget_data)
        widget_record = table.get_item(Key={'id': widget_data['widgetId']})['Item']
        assert widget_record['id'] == widget_data['widgetId']
        assert widget_record['owner'] == widget_data['owner']
        assert widget_record['label'] == widget_data['label']
        assert widget_record['description'] == widget_data['description']
        assert widget_record['size-unit'] == widget_data['otherAttributes'][0]['value']
        assert widget_record['height'] == widget_data['otherAttributes'][1]['value']
        assert widget_record['length'] == widget_data['otherAttributes'][2]['value']
        assert widget_record['price'] == widget_data['otherAttributes'][3]['value']
        assert widget_record['vendor'] == widget_data['otherAttributes'][4]['value']

    @mock_aws
    def test_process_widget_correctlydeletesdynamodata(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=main.bucket_name)
        bucket = conn.Bucket(main.bucket_name)
        test_widget_filename = "test-widget.json"
        test_delete_widget_filename = "test-delete-widget.json"
        bucket.upload_file(test_widget_filename, str(1))
        widget_data = dict()
        with open(test_widget_filename, 'r') as file:
            widget_data = json.load(file)
        dynamodb = boto3.resource('dynamodb')
        dynamodb.create_table(
            TableName=main.table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table = dynamodb.Table(main.table_name)
        main.process_widget(widget_data)
        with open(test_delete_widget_filename, 'r') as file:
            widget_data = json.load(file)
        main.process_widget(widget_data)
        result = table.get_item(Key={'id': widget_data['widgetId']})
        assert 'Item' not in result.keys()

    @mock_aws
    def test_process_widget_correctlyupdatessdynamodata(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=main.bucket_name)
        bucket = conn.Bucket(main.bucket_name)
        test_widget_filename = "test-widget.json"
        test_update_widget_filename = "test-update-widget.json"
        bucket.upload_file(test_widget_filename, str(1))
        widget_data = dict()
        with open(test_widget_filename, 'r') as file:
            widget_data = json.load(file)
        dynamodb = boto3.resource('dynamodb')
        dynamodb.create_table(
            TableName=main.table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        table = dynamodb.Table(main.table_name)
        main.process_widget(widget_data)
        with open(test_update_widget_filename, 'r') as file:
            widget_data = json.load(file)
        main.process_widget(widget_data)
        widget_record = table.get_item(Key={'id': widget_data['widgetId']})['Item']
        assert widget_record['id'] == widget_data['widgetId']
        assert widget_record['owner'] == widget_data['owner']
        assert widget_record['label'] == widget_data['label']
        assert widget_record['description'] == widget_data['description']
        assert widget_record['size-unit'] == widget_data['otherAttributes'][0]['value']
        assert widget_record['height'] == widget_data['otherAttributes'][1]['value']
        assert widget_record['length'] == widget_data['otherAttributes'][2]['value']
        assert widget_record['price'] == widget_data['otherAttributes'][3]['value']
        assert widget_record['vendor'] == widget_data['otherAttributes'][4]['value']



    @mock_aws
    def test_logstofile(self):
        with open(main.log_filename, 'w'): #clear log file contents
            pass
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket=main.bucket_name)
        bucket = conn.Bucket(main.bucket_name)
        test_widget_filename = "test-widget.json"
        bucket.upload_file(test_widget_filename, str(1))
        widget_data = dict()
        with open(test_widget_filename, 'r') as file:
            widget_data = json.load(file)
        dynamodb = boto3.resource('dynamodb')
        dynamodb.create_table(
            TableName=main.table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        main.run(False)
        file = open(main.log_filename, 'r')
        file_contents = file.read();
        file.close()
        assert widget_data['widgetId'] in file_contents



