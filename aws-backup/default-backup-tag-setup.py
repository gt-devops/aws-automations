'''
Create a cloudwatch Schedule
with input json =  {
          "AccountId": "",
          "Region": ""
        }

Environment Vaiable Required
1. DefaultPlanKey 
2. DefaultPlanValue
3. OrganizationServiceRole

Note: OrganizationServiceRole used to manage another account services using role.

What is this script doing?

1. Checking all backup plan Tag Key and Value
2. EC2, EFS, EBS Currently will be auto Tagged with DefaultPlanKey and DefaultPlanValue, if no plan matched


What Next?
1. Auto Tag on each event rather than schedule.
2. Other Services will be added soon
'''

from __future__ import print_function
from boto3.session import Session

import boto3
import os
import json
import time
import botocore
from datetime import datetime

def get_instances(**kwargs):
    instanceDetails = []
    ec2 = kwargs['Session'].resource('ec2')
    #now need to capture all ec2 available in region_specific
    for instance in ec2.instances.all():
        payload = {
                "InstanceId": instance.id,
                "Tags": instance.tags
              }
        instanceDetails.append(payload)
    return instanceDetails


def list_backup_plans(**kwargs):
    nextmarker = None
    done = False
    listData = []
    
    backup = kwargs['Session'].client('backup')

    while not done:
        if nextmarker:
            response = backup.list_backup_plans(NextToken=nextmarker)
        else:
            response = backup.list_backup_plans()

        for backupPlans in response['BackupPlansList']:
            listData.append(backupPlans['BackupPlanId'])

        if 'NextToken' in response:
            nextmarker = response['NextToken']
        else:
            break
        
    return listData

def list_backup_selections(**kwargs):
    nextmarker = None
    done = False
    listData = []
    
    backup = kwargs['Session'].client('backup')
    
    for planId in kwargs['BackupPlans']:
        while not done:
            if nextmarker:
                response = backup.list_backup_selections(NextToken=nextmarker, BackupPlanId=planId)
            else:
                response = backup.list_backup_selections(BackupPlanId=planId)
    
            for data in response['BackupSelectionsList']:
                listData.append(data)
    
            if 'NextToken' in response:
                nextmarker = response['NextToken']
            else:
                break
            
    return listData

def check_tag_availability(**kwargs):
    match = False
    for plans in kwargs['AvailablePlans']:
        for tag in kwargs['ResourceTags']:
            if plans['Key'] == tag['Key'] and plans['Value'] == tag['Value']:
                match = True
                break
        if match:
            break
    return match

def get_tag_info(**kwargs):
    details = []
    backup = kwargs['Session'].client('backup')
    for selection in kwargs['PlanDetails']:
        response = backup.get_backup_selection(
            BackupPlanId=selection['BackupPlanId'],
            SelectionId=selection['SelectionId']
        )
        data = {}
        for tag in response['BackupSelection']['ListOfTags']:
            data['Key'] = tag['ConditionKey']
            data['Value'] = tag['ConditionValue']

        details.append(data)    
    return details


def add_ec2_tags(**kwargs):
    try:
        ec2 = kwargs['Session'].resource('ec2')
        response = ec2.create_tags(
            Resources=kwargs['ResourceIds'],
            Tags=[
                {
                    'Key': os.environ.get('DefaultPlanKey'),
                    'Value': os.environ.get('DefaultPlanValue')
                },
            ]
        )
        return True
    except Exception as e:
        print(e)
        return False
    
def list_file_systems(**kwargs):
    nextmarker = None
    done = False
    listData = []
    
    client = kwargs['Session'].client('efs')
    
    while not done:
        if nextmarker:
            response = client.describe_file_systems(Marker=nextmarker)
        else:
            response = client.describe_file_systems()

        for data in response['FileSystems']:
            listData.append(data)

        if 'NextMarker' in response:
            nextmarker = response['NextMarker']
        else:
            break
            
    return listData

def add_efs_tags(**kwargs):
    efs = kwargs['Session'].client('efs')
    try:
        for efsId in kwargs['EfsIds']:
            response = efs.create_tags(
                FileSystemId=efsId,
                Tags=[
                    {
                        'Key': os.environ.get('DefaultPlanKey'),
                        'Value': os.environ.get('DefaultPlanValue')
                    },
                ]
            )
        return True
    except Exception as e:
        print(e)
        return False

def get_volumes(**kwargs):
    volumes = []
    ec2 = kwargs['Session'].resource('ec2')
    #now need to capture all ec2 available in region_specific
    for volume in ec2.volumes.all():
        payload = {
                "VolumeId": volume.id,
                "Tags": volume.tags
              }
        volumes.append(payload)
    return volumes

def lambda_handler(event, context):
  print(json.dumps(event))
  print("**********************************************")
  account = event['AccountId']
  if(context.invoked_function_arn.split(":")[4] != event['AccountId']):
    if 'AccessRoleName' in event:
        organizationServiceRole = event['AccessRoleName']
    else: 
        organizationServiceRole = os.environ.get('OrganizationServiceRole')
    print(organizationServiceRole)
    stsRoleSessionName = 'genericInstanceFan'
    credentials = assume_role(account, organizationServiceRole, stsRoleSessionName)
    session = boto3.Session(aws_access_key_id=credentials['AccessKeyId'],aws_secret_access_key=credentials['SecretAccessKey'],aws_session_token=credentials['SessionToken'],region_name=event['Region'])
  else:
    session = boto3.Session(region_name=event['Region'])
  
  backupPlans = list_backup_plans(Session=session)
  selectionDetails = list_backup_selections(Session=session, BackupPlans=backupPlans)
  assignedTagsInfo = get_tag_info(Session=session, PlanDetails=selectionDetails)
  assignedTagsInfo.append({"Key": os.environ.get('DefaultPlanKey'), "Value": os.environ.get('DefaultPlanValue')})
  print("Backup Plan Current Tags : ", json.dumps(assignedTagsInfo))
  
  ## For Instance All in Specific Region and ebs volumes
  instanceDetails = get_instances(Session=session)
  #print(instanceDetails)
  #print('**************************')

  print('*********EC2******')
  instanceIds = []
  for instance in instanceDetails:
    isEC2BackupTagged = check_tag_availability(AvailablePlans=assignedTagsInfo, ResourceTags=instance['Tags'])
    print("Tag Status : ", isEC2BackupTagged, "For Instance : ", instance['InstanceId'])
    if not isEC2BackupTagged:
        instanceIds.append(instance['InstanceId'])

  if instanceIds:
      statusTagEc2 = add_ec2_tags(Session=session, ResourceIds=instanceIds)
      print("Tag Created Status : ",statusTagEc2)
  ## for ebs
  print('*********EBS******')
  volumeIds = []
  for volume in get_volumes(Session=session):
    isEBSBackupTagged = check_tag_availability(AvailablePlans=assignedTagsInfo, ResourceTags=volume['Tags'])
    print("Tag Status EBS : ", isEBSBackupTagged, "For EBS : ", volume['VolumeId'])
    if not isEBSBackupTagged:
        volumeIds.append(volume['VolumeId'])
  
  if volumeIds:
      statusTagEBS = add_ec2_tags(Session=session, ResourceIds=volumeIds)
      print("Tag Created Status : ", statusTagEBS)    
  ## For efs
  print('*********EFS******')
  efsIds = []
  for efsDetails in list_file_systems(Session=session):
    isEFSBackupTagged = check_tag_availability(AvailablePlans=assignedTagsInfo, ResourceTags=efsDetails['Tags'])
    print("Tag Status EFS : ", isEFSBackupTagged, "For EFS : ", efsDetails['FileSystemId'])
    if not isEFSBackupTagged:
        efsIds.append(efsDetails['FileSystemId'])

  if efsIds:
      statusTagEFS = add_efs_tags(Session=session, EfsIds=efsIds)
      print("Tag Created Status : ", statusTagEFS)

if __name__ == '__main__':
    event = {
          "AccountId": "",
          "Region": ""
        }
    context = {}
    lambda_handler(event, context)
  