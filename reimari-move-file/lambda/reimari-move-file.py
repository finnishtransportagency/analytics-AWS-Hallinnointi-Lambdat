import boto3
import json
import os
import time

start_time = time.time()

s3 = boto3.resource('s3')

# Fetch environment variables from AWS Lambda Configuration
FULL_SCANS = os.getenv('FULL_SCANS', 'navigointilaji; ')
PREFIX = os.getenv('PREFIX', 'reimari_')
PROD_BUCKET = os.getenv('PROD_BUCKET', '')
TARGET_PREFIX = os.getenv('TARGET_PREFIX', 'reimari_')
FILE_NAMES = os.getenv('FILE_NAMES', '')
CSV_DELIMITER = os.getenv('CSV_DELIMITER', 'pipe')
DEV_BUCKET = os.getenv('DEV_BUCKET', '')
TEST_PREFIX = os.getenv('TEST_PREFIX', 'Testi_')
SEPARATE_TEST_FILES = os.getenv('SEPARATE_TEST_FILES', False)

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
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']

    test = False

    if FILE_NAMES == '':
        list_of_files = default_list_of_files
    else:
        list_of_files = FILE_NAMES.split("; ")
    
    # If target bucket was not defined as an environment variable
    if PROD_BUCKET == '':
        print('PROD_BUCKET not set. Nothing to do.')
        return

    # Declaring the source to be copied
    copy_source = {'Bucket': source_bucket, 'Key': s3_file_name}
    print(copy_source)
    #target_s3 = s3.Bucket(PROD_BUCKET)
    #dev_s3 = s3.Bucket(DEV_BUCKET)
    #print(target_s3)

    # Get filename without extension   
    name = s3_file_name.rsplit('/',1)[1]
    name = name.rsplit('.',1)[0]
    
    # If we have separate test files for DEV data
    if SEPARATE_TEST_FILES:
        # Check if the file is a testfile
        if name.startswith(TEST_PREFIX):
            name = name[len(TEST_PREFIX):]
            test = True
        
    # Checking if the file is on the list of files to process
    if not is_file_on_this_list(name, list_of_files):
        print(name + " is not on the list of files to handle. STOPPING LAMBDA...")
        return

    # Create timestamp in milliseconds
    timestamp = str(round(time.time() * 1000))

    # Check if file is in fullscanned list
    full_scanned = check_full_scan(name)

    print('fullscanned is: ' + full_scanned)

    # Set target name suitable for ADE
    full_name = 'table.' + PREFIX + name + '.' + timestamp + '.batch.' + timestamp + '.fullscanned.' + full_scanned + '.delim.' + CSV_DELIMITER + '.csv'
    target_name = TARGET_PREFIX + name + '/' + full_name
    # table.reimari_file_name.1550837185775.batch.1550837185775.fullscanned.false.delim.pipe.csv

    #destination_bucket = ""

    destination_buckets = []

    # If not a testfile, use prod bucket
    if not test:
        destination_buckets.append(PROD_BUCKET)

        # If separate test files aren't in use both buckets
        if not SEPARATE_TEST_FILES:
            destination_buckets.append(DEV_BUCKET)

    # If it is a testfile (also means we have SEPARATE_TEST_FILES == True)         
    else:
        destination_buckets.append(DEV_BUCKET)
    
    
    # Copying to DEV, PROD or both bukcets depending on the previous conditions
    for bucket in destination_buckets:

        target_s3 = s3.Bucket(bucket)

        # Copying the files to target S3
        print("Trying to copy file to " + bucket + "/" + target_name)
        try:
            target_s3.copy(copy_source, target_name, ExtraArgs={'ACL': 'bucket-owner-full-control'})
        except Exception as e:
            raise Exception( "Unexpected error: " + e.__str__())
        print('Copy successful!')

    print("Done in %s seconds." % (time.time() - start_time))

    
    
# Checks if file name is in FULL_SCANS list. List is constructed so that "; " separates files. Environment variable set in lambda
def check_full_scan(name):
    full_scan_list = FULL_SCANS.split('; ')
    for val in full_scan_list:
        if val == name:
            return 'true'
    return 'false'

def is_file_on_this_list(filu, lista, prefx=""):
    for f in lista:
        if prefx + f == filu:
            return True
    return False
