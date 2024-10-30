import boto3
import json
import time
import datetime
import sys
import logging

session = boto3.Session()
s3_client = session.client('s3')
s3_resource = boto3.resource('s3')
dynamo_client = session.client('dynamodb')
dynamodb = boto3.resource('dynamodb')

bucket_name = 'usu-cs5250-slade-requests'
table_name = 'widgets'
bucket = s3_resource.Bucket(bucket_name)
table = dynamodb.Table(table_name)
tmp_filename = 'temp-file.json'
log_filename = "assn6-log.log"
logger = logging.getLogger()
logging.basicConfig(filename=log_filename, level=logging.INFO)

def get_widget_data():
    widget_objects = list(bucket.objects.all())
    if len(widget_objects) == 0:
        return None
    widget_key = str(widget_objects[0].key)
    bucket.download_file(widget_key, tmp_filename)
    data = read_data_from_file(tmp_filename)
    s3_client.delete_object(Bucket=bucket_name, Key=widget_key)
    return data

def read_data_from_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data


def process_widget(widget):
    if widget['type'] == 'create':
        process_create_request(widget)
        logger.info("Processed create request")
        print(f"{datetime.datetime.now()} Processed a create request for widget {widget['widgetId']} in request {widget['requestId']}")


def process_create_request(widget):
    record_dict = dict()
    record_dict['id'] = widget['widgetId']
    for key in widget.keys():
        if key != "type" and key != "requestId" and key != "widgetId":
            if key != "otherAttributes":
                record_dict[key] = widget[key]
            else:
                record_dict.update(process_request_other_attributes(widget[key]))
    table.put_item(Item=record_dict)


def process_request_other_attributes(attributes):
    attribute_dict = dict()
    for attribute in attributes:
        attribute_dict[attribute['name']] = attribute['value']
    return attribute_dict


def run():
    not_found_count = 0
    logger.info("Processing Widgets")
    while not_found_count < 10:
        widget = get_widget_data()
        if not widget:
            not_found_count += 1
            logger.info("Waiting for more widgets")
            time.sleep(0.1)
        else:
            logger.info(f"Recieved Widget {widget['widgetId']}")
            logger.info(str(widget))
            not_found_count = 0
            process_widget(widget)
    logger.info("Finished processing all widgets in the request bucket")


logger.info("Started Consumer Application")
logger.info("Reading Command Line Arguments")
try:
    arg_dict = dict()
    for i in range(len(sys.argv)):
        arg = sys.argv[i]
        if arg.startswith('-'):
            arg_dict[arg] = sys.argv[i+1]
    for arg in arg_dict.keys():
        if arg == "-rb":
            bucket_name = arg_dict[arg]
        elif arg == "-dwt":
            table_name = arg_dict[arg]
    if "-rb" not in arg_dict.keys():
        msg = f"Command line argument for request bucket not supplied. Using default: {bucket_name}"
        logger.info(msg)
        print(msg)
    if "-dwt" not in arg_dict.keys():
        msg = f"Command line argument for dynamo table not supplied. Using default: {table_name}"
        logger.info(msg)
        print(msg)

except:
    print("Improper arguments given.")
    logger.error("Invalid command line arguments were supplied")

try:
    run()
except:
    print("An error occurred.")
    logger.error("An error occurred.")