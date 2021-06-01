import boto3
import json
from datetime import datetime, timedelta, timezone
from dateutil.tz import *
import logging
import os

import copy


logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s %(funcName)s', level=logging.INFO,force=True)
err_message="Error Found in Function:"


bucket_name = os.environ.get('S3_BUCKET_NAME')
s3_client = boto3.client('s3',region_name='us-east-2')
lakeformation_client = boto3.client('lakeformation',region_name='us-east-2')
s3_prefix = "lambda-lakeformation-datalocations-grant/checkpoint/"
global boolean_check


        
def lambda_handler(event, context):

    
    # print out input varibles to lambda
    logging.info("this is input from environ variable {}".format(bucket_name))
    logging.debug("this is input from environ variable".format(s3_client))
    
    # look for checkpoint/ prefix in given bucket
    s3_list_res = s3_list_call()
    

    # create checkpoint prefix, if it does not exist
    # this can happen for first run of the Lambda
    if ("Contents" not in s3_list_res.keys()):
        logging.info("checkpoint prefix does not exist, creating it")
        s3_client.put_object(Bucket=bucket_name,Body='', Key=s3_prefix)
        s3_list_res = s3_list_call()
    # key for the checkpoint file on s3 (last_run_datetime.txt)
    # also the date_format info it will contain
    last_run_datetime_key = s3_prefix + "last_run_datetime.txt"
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.info("last_run_datetimekey {0},date_format {1}".format(last_run_datetime_key,date_format))
    
    # extract datetime in correct format and in UTC time as followed by AWS API calls
    # also looks for existing last_run_datetime.txt, if not found returns a current time in UTC
    utc_last_run_datetime = extract_last_run_datetime(s3_list_res,last_run_datetime_key,date_format)
    logging.info("Last Run Time from S3 CheckPoint {}".format(utc_last_run_datetime))
    

    lakeformation_list_res = lakeformation_list_call()
    logging.info("lakeformation list resource call completed")
    
    # List of Targets and Catalog ID , need to be input from env variables?????
    list_of_targets = os.environ.get('LIST_OF_TARGETS').split(",")
    catalog_id = os.environ.get('CATALOG_ID')
    logging.info("catalog_id {}".format(catalog_id))
    logging.info("target ids {}".format(list_of_targets))
    
    # grant dictionary template
    # this will be modified to fit the needs of grant batch permission api call
    grant_dict  = {
            'Id': 'DL Permission',
            'Principal': {
                'DataLakePrincipalIdentifier': ''
            },
            'Resource': {
                'DataLocation': {
                    'CatalogId': catalog_id,
                    'ResourceArn': ''
                }
            },
            'Permissions': [
                'DATA_LOCATION_ACCESS'
            ],
            'PermissionsWithGrantOption': [
                'DATA_LOCATION_ACCESS'
            ]
        }
    
    # Returns list of entries containing grant dictionary with their respective targets
    list_of_entries = lakeformation_create_entries_list(list_of_targets,grant_dict)
    
    
    boolean_check=False
    boolean_list = []
    # Iterate through resource arns and do the grant permission call for the targets
    for resource_datetime,resource_arn in extract_resoruce_datetime_arn(lakeformation_list_res):
        boolean_check = compare_datetime(resource_datetime,resource_arn,utc_last_run_datetime,list_of_entries,catalog_id,boolean_check)
        boolean_list.append(boolean_check)
    
    if(not any(boolean_list)):
        logging.info("****No New Data Locations Granted Access in Current Run*****")
    # On No Errors Update the last_run_datetime_key with latest timestamp.
    datetime_curr = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    object = s3_client.put_object(Bucket = bucket_name, Body = str.encode(datetime_curr), Key = last_run_datetime_key)



"****************************************************************************************************"   

def s3_list_call():
    '''
    Desc: Does s3 list objects call - looks for checkpoint/ prefix

    Return: Response back from S3 api call made
    '''
    try:
        s3_list_res = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=s3_prefix
            )
        return s3_list_res
    except Exception as e:
        logging.error(err_message)
        raise e
        
        
def extract_last_run_datetime(s3_list_res,last_run_datetime_key,date_format):
    '''
    Desc: 
    Reads datetime from s3 last_run_datetime.txt file. Also, Create one if it does not exist.
    Build or Generate a UTC formatted timestamp.

    Params: 
    s3_list_res = response from previous s3_list_call() function
    last_run_datetime_key = key to look for checkpoint/last_run_datetime.txt
    date_format = date format to be parsed with

    Return:
    utc_last_run_datetime = timestamp used to compare for any new databases to be shared (in UTC format)

    '''
    if( len(s3_list_res.get('Contents')) == 2):
        for i in s3_list_res.get('Contents'):
            if(i['Key'] == last_run_datetime_key):
                try:
                    # if the last_modified.txt file exists
                    last_run_datetime=s3_client.get_object(Bucket = bucket_name, Key = last_run_datetime_key)
                    decoded_last_run_datetime=last_run_datetime['Body'].read().decode('utf-8')
                    formatted_last_run_datetime = datetime.strptime(decoded_last_run_datetime, date_format )
                    utc_last_run_datetime = formatted_last_run_datetime.replace(tzinfo=tzlocal())
                except ValueError as e:
                    logging.error("Invalid date format, Unable to Parse Date")
                    raise e
    if( len(s3_list_res.get('Contents')) == 1):

        # if the last_modified.txt file does not exist
        # this can happen on first run of lambda
        logging.info("Last Run Datetime file does not exist")
        utc_last_run_datetime=""
            
    return utc_last_run_datetime

def lakeformation_list_call():
    '''
    Desc: 
    List resources available using boto3 lake formation client API call
    
    Return:
    Response from the API call
    '''
    try:
        lakeformation_list_res = lakeformation_client.list_resources(
                    FilterConditionList=[
                        {
                            'Field': 'RESOURCE_ARN',
                            'ComparisonOperator': 'CONTAINS',
                            'StringValueList': [
                                'arn:aws:s3:::'
                            ]
                        }
                    ],
                    MaxResults=123
                )
        return lakeformation_list_res
    except Exception as e:
        logging.error(err_message)
        raise e
    
def lakeformation_create_entries_list(list_of_targets,grant_dict):
    '''
    Desc:
    build a list with multiple grant dicts formatted with the targets individually

    Params:
    list of targets: list of the targets coming in as input
    grant_dict: dictionary which will be formatted and appended to the list_of_entries

    Return:
    List_of_entries: list of all grant dicts having respective targets
    '''
    try:
        list_of_entries = []
        for target_identifier in list_of_targets:
            grant_dict['Id']= target_identifier
            grant_dict['Principal']['DataLakePrincipalIdentifier'] = target_identifier
            list_of_entries.append(copy.deepcopy(grant_dict))
        return list_of_entries
    except Exception as e:
        logging.error(err_message)
        raise e
 
def extract_resoruce_datetime_arn(lakeformation_list_res):
    '''
    Desc:
    Extract resource_datetime and resource_arn from list of resources available in lake formation
    Each of them will be yielded one at a time

    Params:
    lakeformation_list_res =  list of resources in lake formation taken as input

    Yield:
    resource_datetime,resource_arn = datetime and arn for each resource are returned
    '''
    try:
        for i in lakeformation_list_res['ResourceInfoList']:
            resource_datetime = i.get('LastModified')
            resource_arn = i.get('ResourceArn')

            yield resource_datetime,resource_arn
    except Exception as e:
        logging.error(err_message)
        raise e

def lakeformation_data_location_grant(list_of_entries,catalog_id):
    '''
    Desc:
    Make a lake formation grant permission call for the list of entries and catalog id provided
    Params:
    list_of_entries: list with all the dictionaries for each target and resource arn
    catalog_id: value for catalog_id
    '''
    try:
        response2 = lakeformation_client.batch_grant_permissions(
            Entries=list_of_entries
        )
        if (len(response2['Failures']) != 0):
            logging.info(response2['Failures'])
            raise Exception("Failures Found Batch Grant Permission API call")
    except Exception as e:
        logging.error(err_message)
        raise e

def compare_datetime(resource_datetime,resource_arn,utc_last_run_datetime,list_of_entries,catalog_id, boolean_check):
    '''
    Desc:
    Compare timestamps and if its a new resource, do the grant permission call with appropriate resource arn

    Params:
    resource_datetime: datetime of the resource creation
    resource_arn: resource arn
    utc_last_run_datetime: last run time of the lambda
    list_of_entries: entries with targets attached to them
    catalog_id : catalog id

    '''
    try:
        if(utc_last_run_datetime == ""):
            call_grant_permission(list_of_entries,resource_arn,catalog_id,resource_datetime)
            boolean_check = True
            return boolean_check
        if(utc_last_run_datetime != ""):
            if resource_datetime > utc_last_run_datetime:
                boolean_check=True
                call_grant_permission(list_of_entries,resource_arn,catalog_id,resource_datetime)
                return boolean_check
        return False
    except Exception as e:
        logging.error(err_message)
        raise e
        
def call_grant_permission(list_of_entries,resource_arn,catalog_id,resource_datetime):
    for grant_dict_entry in list_of_entries:
        grant_dict_entry['Resource']['DataLocation']['ResourceArn'] = resource_arn
    
    lakeformation_grant_res =lakeformation_data_location_grant(list_of_entries,catalog_id)
    logging.info("List of New Entries from Previous Run:***** ResourceArn {0}, LastModified {1}******".format(resource_arn,resource_datetime))        
