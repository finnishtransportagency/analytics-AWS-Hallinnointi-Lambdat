import boto3
import json
import os
import time

s3 = boto3.resource('s3')

# Fetch environment variables from AWS Lambda Configuration
FULL_SCANS = os.getenv('FULL_SCANS', 'navigointilaji__')
PREFIX = os.getenv('PREFIX', 'reimari_')
TARGET_BUCKET = os.getenv('TARGET_BUCKET', '')
TARGET_PREFIX = os.getenv('TARGET_PREFIX', 'reimari_')
TEMPLATE_BUCKET = os.getenv('TEMPLATE_BUCKET', 'sftp-siirto-yhteinen')
TEMPLATE_PREFIX = os.getenv('TEMPLATE_PREFIX', 'templates/')
FILE_NAMES = os.getenv('FILE_NAMES', '')

default_list_of_files = [
    "navigointilaji",
    "turvalaite",
    "tyyppi",
    "t_component",
    "t_comp_class",
    "t_safety_device",
    "t_safety_device_observation",
    "t_sdgroup_sd",
    "t_sd_component",
    "t_sd_group",
    "t_sd_obs_solid",
    "vayla",
    "vayla_tlyhteys"
                ]

def lambda_handler(event, context):
    
    print('Starting lambda...')
    # Retrieve File Information
    source_bucket =   event['Records'][0]['s3']['bucket']['name']
    s3_file_name =  event['Records'][0]['s3']['object']['key']

    if FILE_NAMES == '':
        list_of_files = default_list_of_files
    else:
        list_of_files = FILE_NAMES.split(";")
    
    # If target bucket was not defined as an environment variable
    if TARGET_BUCKET == '':
        print('TARGET_BUCKET not set. Nothing to do.')
        return

    # Declaring the source to be copied
    copy_source = {'Bucket': source_bucket, 'Key': s3_file_name}
    print(copy_source)
    target_s3 = s3.Bucket(TARGET_BUCKET)
    #print(target_s3)

    # Get filename without extension   
    name = s3_file_name.rsplit('/',1)[1]
    name = name.rsplit('.',1)[0]
    
    if not is_file_on_this_list(name, list_of_files):
        print(name + " is not on the list of files. STOPPING LAMBDA...")
        return


    # Create timestamp in milliseconds
    timestamp = str(round(time.time() * 1000))

    # Check if file is in fullscanned list
    full_scanned = check_full_scan(name)

    print('fullscanned is: ' + full_scanned)

    # Set target name suitable for ADE
    full_name = 'table.' + PREFIX + name + '.' + timestamp + '.batch.' + timestamp + '.fullscanned.' + full_scanned + '.delim.pipe.csv'
    target_name = TARGET_PREFIX + name + '/' + full_name

    # Copying the files to target S3
    print("Trying to copy file to " + TARGET_BUCKET + "/" + target_name)
    try:
        target_s3.copy(copy_source, target_name)
    except Exception as e:
        raise Exception( "Unexpected error: " + e.__str__())
    print('Copy successful!')
    
# Checks if file name is in FULL_SCANS list. List is constructed so that __ separates files. Environmentalvariable set in lambda
def check_full_scan(name):
    full_scan_list = FULL_SCANS.split('__')
    for val in full_scan_list:
        if val == name:
            return 'true'
    return 'false'

def is_file_on_this_list(filu, lista):
    for f in lista:
        if f == filu:
            return True
    return False