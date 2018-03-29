import argparse
import boto3, botocore
import time
import sys
import json
from datetime import datetime

def to_epochms(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000)

def get_log_events(client, log_group, log_stream, timestamp):
    try:
         response = client.get_log_events(
             logGroupName=log_group,
             logStreamName=log_stream,
             startTime=timestamp,
             startFromHead=True
         )
    except client.exceptions.ResourceNotFoundException:
        return []
    return response['events']

#
# Main
#
parser = argparse.ArgumentParser()
parser.add_argument('task_json', help='Task configuration json')
parser.add_argument('-i', '--interval', help='polling interval', type=int, default=20)
parser.add_argument('-t', '--timeout', help='end after TIMEOUT seconds', type=int, default=300)
args = parser.parse_args()

task_config = None
with open(args.task_json, 'r') as f:
    task_config = json.loads(f.read())

ecs  = boto3.client('ecs')
logs = boto3.client('logs')

start_time = datetime.utcnow()

# Get log info from task definition
response = ecs.describe_task_definition(
    taskDefinition=task_config['taskDefinition']
)
container         = response['taskDefinition']['containerDefinitions'][0]
container_name    = container['name']
log_opts          = container['logConfiguration']['options']
log_group         = log_opts['awslogs-group']
log_region        = log_opts['awslogs-region']
log_stream_prefix = log_opts['awslogs-stream-prefix']

print 'Running migration task.'
response = ecs.run_task(
    cluster=task_config['cluster'],
    taskDefinition=task_config['taskDefinition'],
    count=task_config['count'],
    launchType=task_config['launchType'],
    networkConfiguration=task_config['networkConfiguration']
)

task_arn = response['tasks'][0]['taskArn']
task_id = task_arn.split('/')[-1]
log_stream = '/'.join([log_stream_prefix, container_name, task_id])
last_log_timestamp = to_epochms(start_time)
elapsed_time = None
task_data    = None
while True:
    print 'Sleeping {} seconds'.format(args.interval)
    time.sleep(args.interval)

    log_events = get_log_events(logs, log_group, log_stream, last_log_timestamp)

    for e in log_events:
        timestr = datetime.utcfromtimestamp(e['timestamp'] / 1000).isoformat()
        print '[{}]: {}'.format(timestr, e['message'])
    if len(log_events):
        last_log_timestamp = log_events[-1]['timestamp'] + 1

    # Check if task timed out
    elapsed_time = datetime.utcnow() - start_time
    if elapsed_time.seconds > args.timeout:
        print 'Timed out after {} seconds'.format(elapsed_time.seconds)
        sys.exit(1)

    task_data = ecs.describe_tasks(
        cluster=task_config['cluster'],
        tasks=[ task_arn ]
    )

    container   = task_data['tasks'][0]['containers'][0]
    last_status = container['lastStatus']
    exit_code = 0
    if 'exitCode' in container:
        exit_code = container['exitCode']

    # Check if task is done.
    if last_status == 'STOPPED':
        if exit_code != 0:
            print 'Migration failed.'
            sys.exit(exit_code)
        else:
            break

print "Migration complete after {} seconds".format(elapsed_time.seconds)
