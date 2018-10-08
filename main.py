import boto3
import json
import os
from task2 import ca_bkp_task

prod_key = os.environ['prod_key']
prod_secret = os.environ['prod_secret']
prod_sqs = os.environ['prod_sqs']
prod_region = os.environ['prod_region']

def main():

  sqs = boto3.resource('sqs', region_name=prod_region, aws_access_key_id=prod_key, aws_secret_access_key=prod_secret)
  queue = sqs.get_queue_by_name(QueueName=prod_sqs)
  messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1)
  rds = []

  while len(messages) > 0:

    for msg in messages:
      msg_body1 = json.loads(msg.body)
      msg_body2 = json.loads(msg_body1['Message'])
      if msg_body2['Event ID'][-14:] == 'RDS-EVENT-0002':
        rds.append({
          'Name': msg_body2['Source ID'],
          'Time': msg_body2['Event Time']
        })
        ca_bkp_task.delay(msg_body2['Source ID'])
      msg.delete()

    messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=1)

  print(rds)

if __name__ == "__main__":
  main()

