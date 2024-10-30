import boto3

from moto import mock_aws
import main
import json
import unittest


class TestClass(unittest.TestCase):

    @mock_aws
    def test_get_widget_data_returns_data(self):
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




