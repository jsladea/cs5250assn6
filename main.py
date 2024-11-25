import boto3
import json
import time
import datetime
import sys
import logging

session = boto3.Session()
s3_client = session.client('s3')
s3_resource = boto3.resource('s3')
sqs = boto3.resource('sqs')
dynamodb = boto3.resource('dynamodb')

queue_name = 'cs5250-requests'
bucket_name = 'usu-cs5250-slade-requests'
table_name = 'widgets'
bucket = s3_resource.Bucket(bucket_name)
table = dynamodb.Table(table_name)
queue = sqs.get_queue_by_name(QueueName=queue_name)
tmp_filename = 'temp-file.json'
log_filename = "assn6-log.log"
logger = logging.getLogger()
logging.basicConfig(filename=log_filename, level=logging.INFO)

def reconfigure_resources():
    global bucket, table, queue
    bucket = s3_resource.Bucket(bucket_name)
    table = dynamodb.Table(table_name)
    queue = sqs.get_queue_by_name(QueueName=queue_name)


def get_widget_data_sqs():
    msgs = queue.receive_messages(VisibilityTimeout=35, WaitTimeSeconds=5)
    return msgs[0] if len(msgs) > 0 else None


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
    elif widget['type'] == 'update':
        process_create_request(widget)
        logger.info("Processed update request")
        print(f"{datetime.datetime.now()} Processed an update request for widget {widget['widgetId']} in request {widget['requestId']}")
    elif widget['type'] == 'delete':
        if process_delete_request(widget):
            logger.info("Processed delete request")
            print(f"{datetime.datetime.now()} Processed a delete request for widget {widget['widgetId']} in request {widget['requestId']}")


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


def process_delete_request(widget):
    key = {
        "id": widget['widgetId']
    }
    try:
        table.delete_item(Key=key)
    except:
        msg = f"Failed to delete widget ({widget['widgetId']})."
        logger.error(msg)
        print(msg)
        return False
    return True


def process_request_other_attributes(attributes):
    attribute_dict = dict()
    for attribute in attributes:
        attribute_dict[attribute['name']] = attribute['value']
    return attribute_dict


def run(use_queue):
    not_found_count = 0
    logger.info("Processing Widgets")
    wait_count = 10 if not use_queue else 5
    while not_found_count < wait_count:
        widget = None
        msg = None
        if not use_queue:
            widget = get_widget_data()
        else:
            msg = get_widget_data_sqs()
            widget = None if not msg else json.loads(msg.body)
        if not widget:
            not_found_count += 1
            logger.info("Waiting for more widgets")
            time.sleep(0.1)
        else:
            logger.info(f"Recieved Widget {widget['widgetId']}")
            logger.info(str(widget))
            not_found_count = 0
            process_widget(widget)
            if use_queue:
                msg.delete()
    logger.info(f"Finished processing all widgets in the request {'bucket' if not use_queue else 'queue'}")


def read_command_and_init_config():
    logger.info("Reading Command Line Arguments")
    arg_dict = dict()
    for i in range(len(sys.argv)):
        arg = sys.argv[i]
        if arg.startswith('-'):
            arg_dict[arg] = sys.argv[i + 1]
    for arg in arg_dict.keys():
        if arg == "-rb":
            bucket_name = arg_dict[arg]
        elif arg == "-dwt":
            table_name = arg_dict[arg]
        elif arg == "-rq":
            queue_name = arg_dict[arg]
    reconfigure_resources()
    return arg_dict


def process_command():
    try:
        arg_dict = read_command_and_init_config()

        if "-dwt" not in arg_dict.keys():
            msg = f"Command line argument for dynamo table not supplied. Using default: {table_name}"
            logger.info(msg)
            print(msg)
        if "-rb" in arg_dict.keys() and "-rq" in arg_dict.keys():
            msg = f"Command line arguments for request bucket (-rb) and request queue (-rq) supplied. You may only use one."
            logger.error(msg)
            print(msg)
            return False, False
        if "-rb" not in arg_dict.keys() and "-rq" not in arg_dict.keys():
            msg = f"Command line argument for request bucket not supplied, and no argument for for request queue. Using default request bucket: {bucket_name}"
            logger.info(msg)
            print(msg)
        return True, "-rq" in arg_dict.keys()
    except:
        print("Improper arguments given.")
        logger.error("Invalid command line arguments were supplied")
        return False, False


if __name__ == "__main__":
    try:
        logger.info("Started Consumer Application")
        result = process_command()
        if result[0]:
            run(result[1])
    except:
        print("An error occurred.")
        logger.error("An error occurred.")