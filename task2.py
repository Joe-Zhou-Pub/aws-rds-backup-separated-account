from celery import Celery
import boto3
import botocore
import json
import os
import datetime
import re
import time

prod_key = os.environ['prod_key']
prod_secret = os.environ['prod_secret']
prod_region = os.environ['prod_region']
prod_account = os.environ['prod_account']
prod_kmskey = os.environ['prod_kmskey']

bkp_account = os.environ['bkp_account']
bkp_kmswest = os.environ['bkp_kmswest']
bkp_kmseast = os.environ['bkp_kmsest']
bkp_region = os.environ['bkp_region']
kept_num = os.environ['bkp_copy_num']

def byTimestamp(snap):
  if 'SnapshotCreateTime' in snap:
    return datetime.datetime.isoformat(snap['SnapshotCreateTime'])
  else:
    return datetime.datetime.isoformat(datetime.datetime.now())

def waitSnap(client, snap_id):
  counter = 1
  snaps = client.describe_db_snapshots(DBSnapshotIdentifier=snap_id,SnapshotType='manual')
  while snaps['DBSnapshots'][0]['Status'] != 'available' :
     print("still waiting for %s" % snaps['DBSnapshots'][0]['DBSnapshotArn'])
     time.sleep(360)
     counter += 1
     if counter > 80:
       print('Waiting on snapshop longer than 8 hours')
       raise
     snaps = client.describe_db_snapshots(DBSnapshotIdentifier=snap_id,SnapshotType='manual')

app = Celery('task2', broker='pyamqp://guest@localhost//')

@app.task
def ca_bkp_task(rds_instance):

      # Copy the latest auto-snapshop with KMS key
  prod_client = boto3.client('rds', region_name=prod_region, aws_access_key_id=prod_key, aws_secret_access_key=prod_secret)
  auto_snaps = prod_client.describe_db_snapshots(DBInstanceIdentifier=rds_instance,SnapshotType='automated')['DBSnapshots']
  auto_snap = sorted(auto_snaps, key=byTimestamp, reverse=True)[0]['DBSnapshotIdentifier']
  auto_snap_arn = 'arn:aws:rds:%s:%s:snapshot:%s' % (prod_region, prod_account, auto_snap)
  backup_snap_id = (re.sub('rds:', 'ca-bkp2-', auto_snap))
  backup_snap_arn = 'arn:aws:rds:%s:%s:snapshot:%s' % (prod_region, prod_account, backup_snap_id)
  source_snap_arn = 'arn:aws:rds:%s:%s:snapshot:%s' % (prod_region, bkp_account, backup_snap_id)

  try:
    response = prod_client.copy_db_snapshot(
      SourceDBSnapshotIdentifier=auto_snap_arn,
      TargetDBSnapshotIdentifier=backup_snap_id,
      KmsKeyId=prod_kmskey
    )

  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == 'DBSnapshotAlreadyExists':
      print('the snapshot %s already exits, continue' % backup_snap_id)
    else:
      #sns_c.publish(TopicArn=sns_arn, Message=str(e))
      print(e)
      raise

  waitSnap(prod_client, backup_snap_id)

      # Share out to the destination account

  try:
    response = prod_client.modify_db_snapshot_attribute(
      DBSnapshotIdentifier=backup_snap_id,
      AttributeName='restore',
      ValuesToAdd=[bkp_account]
    )
  except botocore.exceptions.ClientError as e:
    print(e)
    raise

      # Copy to local account with KMS key

  bkp_client = boto3.client('rds', region_name=prod_region)
  target_snap_id = backup_snap_id.split(':')[-1]

 try:
    response = bkp_client.copy_db_snapshot(
      SourceDBSnapshotIdentifier=backup_snap_arn,
      TargetDBSnapshotIdentifier=target_snap_id,
      KmsKeyId=bkp_kmswest
    )
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == 'DBSnapshotAlreadyExists':
      print('the snapshot %s already exits, continue' % target_snap_id)
    else:
      print(e)
      raise

  waitSnap(bkp_client, target_snap_id)

      # Delete

  try:
    response = prod_client.delete_db_snapshot(DBSnapshotIdentifier=backup_snap_id)
  except botocore.exceptions.ClientError as e:
    print(e)
    #sns_c.publish(TopicArn=sns_arn, Message=str(e))
    raise

      # Copy to another region with KMS key

  target_snap_id = source_snap_arn.split(':')[-1]
  target_client  = boto3.client('rds', region_name=bkp_region)

  try:
    response = target_client.copy_db_snapshot(
      SourceDBSnapshotIdentifier=source_snap_arn,
      TargetDBSnapshotIdentifier=target_snap_id,
      KmsKeyId=bkp_kmseast,
      SourceRegion=prod_region
    )
  except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == 'DBSnapshotAlreadyExists':
      print('the snapshot %s already exits, continue' % target_snap_id)
    else:
      print("Could not copy: %s" % e)
      raise

  waitSnap(target_client, target_snap_id)

      # Remove old copies

  source_snaps = bkp_client.describe_db_snapshots(DBInstanceIdentifier=rds_instance,SnapshotType='manual')['DBSnapshots']
  length = len(source_snaps)
  if kept_num < length:
    sorted_snaps = sorted(source_snaps, key=byTimestamp, reverse=True)
    for old_snap in sorted_snaps[kept_num:length]:
      try:
        old_snap_id = old_snap['DBSnapshotIdentifier']
        response = bkp_client.delete_db_snapshot(DBSnapshotIdentifier=old_snap_id)
      except botocore.exceptions.ClientError as e:
        print(e)
        raise

  target_snaps = target_client.describe_db_snapshots(DBInstanceIdentifier=rds_instance,SnapshotType='manual')['DBSnapshots']
  length = len(target_snaps)
  if kept_num < length:
    sorted_snaps = sorted(target_snaps, key=byTimestamp, reverse=True)
    for old_snap in sorted_snaps[kept_num:length]:
      try:
        old_snap_id = old_snap['DBSnapshotIdentifier']
        response = target_client.delete_db_snapshot(DBSnapshotIdentifier=old_snap_id)
      except botocore.exceptions.ClientError as e:
        print(e)
        raise

  print('have completed the backup of %s!' %(rds_instance))

