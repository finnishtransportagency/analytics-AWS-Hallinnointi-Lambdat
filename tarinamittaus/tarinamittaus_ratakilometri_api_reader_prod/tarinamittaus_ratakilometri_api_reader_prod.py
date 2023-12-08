import os
import requests
import json
import boto3
import datetime
import time
import csv
import io
import gzip
import logging


secretname = os.environ['secretname']
output_bucket = os.environ['output_bucket']
project = os.environ['project']
project_suffix = os.environ['project_suffix']
landing_bucket = os.environ['landing_bucket']
csv_delim = ';'
logger = logging.getLogger()


def get_secret(secretname: str):
    region_name = 'eu-central-1'
    status = False
    url = None

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secretname)
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            url = secret['url_ratakilometrit']
        
        status = True
    except Exception as e:
        print("Secret {} retrieve error: {}".format(secretname, e))
        

    return status, url

def current_millisecond_time():
    '''Generates current time in milliseconds as unix time'''
    return str(int(round(time.time() * 1000)))
        
def current_date():
    '''Returns current date as a string (YYYY/MM/DD)'''   
    dt = datetime.datetime.now()
    time = dt.strftime("%Y/%m/%d")
    return time #Palauttaa yksinhumeroiset arvot kaksinumeroisina

def integer_check(column_name: str, value: int, row_value: json):
    '''
    Checks if a given value can be converted into integer and returns True or False to be appended to the value_list checked in the function below.
    '''

    if value.isdigit():
        return True    
    else:
        print(f'Error with value of column "{column_name}". Value = {value} and can not be converted to integer.')
        print(f'Affected row will be marked as process_status = 0 and it will require fixing to be loaded to DV. Affected row value = {row_value}')
        return False

def check_if_all_true(value_list: list, row: json):
    '''
    Checks if all conversions were correct. Which means all values in value_list need to be true.
    Value list name is 'value_conversion_check' outside of this function.
    '''

    #print(f'Row before adding status = {row}')
    if all(value_list):
        row['process_status'] = 1
    else:
        row['process_status'] = 0
    #print(f'Row as being returned = {row}')
    return row

def geometria_error_print(value: str, row_value: json):
    #Error print for repeated error structure with geometry values
    print(f'Error with value of column "geometry". Value = {value} and is not a valid WKT format required for datatype geography for snowflake.')
    print(f'Affected row will be marked as process_status = 0 and it will require fixing to be loaded to datavault. Affected row value = {row_value}')

def csv_parse(source_data: json):
    '''
    Parses the csv: creates a generator of the csv as a dictionary object. 
    Changes the positions of longitude and latitude in the csv for snowflake. Returns the file as csv.
    '''

    print('Starting to parse the object returned from the API request.')

    csv_file = io.StringIO(source_data)
    reader = csv.DictReader(csv_file)
    modified_data = []

    print('Checking that datatypes are valid. Looking for geometry colums and reformatting them from "POINT (lat long)" to "POINT (long lat)" for snowflake. ')
    
    #source file columns: FID,internal_id,route_name,kmm,km,m,geometry
    for row in reader:
        value_conversion_check = []

        internal_id = row['internal_id'].strip()
        km = row['km'].strip()
        m = row['m'].strip()
        geometria = row['geometry'].strip() #POINT (60.178955636695406 24.939220724025976)
        #print('Geometria: ' + str(geometria))

        value_conversion_check.append(integer_check('internal_id', internal_id, row))
        value_conversion_check.append(integer_check('km', km, row))
        value_conversion_check.append(integer_check('m', m, row))

        if geometria.startswith('POINT'): #Should be as 'POINT (lat long)'
            geometria_split = geometria.replace('(','').replace(')','').split(' ')
            #print(f'Geometria Split: {geometria_split}')
            if len(geometria_split) == 3:
                point = geometria_split[0]
                longitude = geometria_split[2]
                latitude = geometria_split[1]
                row['geometry'] = f'{point} ({longitude} {latitude})' #correct order for snowflake
            else:
                geometria_error_print(geometria, row)
                value_conversion_check.append(False)
                row_checked = check_if_all_true(value_conversion_check, row)
                modified_data.append(row_checked)
                continue

        elif geometria == 'GEOMETRYCOLLECTION EMPTY':
                row['geometry'] = ''
        else: #This else is only for unexpected errors
            value_conversion_check.append(False)
            geometria_error_print(geometria, row)

        #check if the rows status should be 0 or 1.
        checked_row = check_if_all_true(value_conversion_check, row)
        modified_data.append(checked_row)

    #print(f'Modified data:')
    #print(f'{modified_data}')
    print('Geometry values transformed. Now preparing the file for saving to s3.')
    output_csv = io.StringIO()
    headers = reader.fieldnames
    headers.append('process_status') #Adds the process_status added to variable "row" to the header list
    writer = csv.DictWriter(output_csv, fieldnames=headers, delimiter=csv_delim)
    writer.writeheader()
    writer.writerows(modified_data)
    #print(f'Modified CSV file: \n{csv_file}')
    return output_csv
    

def write_raw_to_s3(s3, csv_file_output, project, suffix, bucket, date):
    '''
    Writes the raw version of the csv to s3 before any parsing is done.
    '''

    print(f'Preparing to upload raw version of {project}: {suffix}')
    file_name = f'{project}_{suffix}_raw_{date.replace("/","-")}.csv.gz'
    object_key = f'{project}/{suffix}_raw/{date}/{file_name}'

    try:
        print("Gzipping raw object")
        csv_file_bytes = csv_file_output.encode('utf8') #Encode the string value
        csv_file_zipped = gzip.compress(csv_file_bytes)
    except Exception as e:    
        print(f'Unable to gzip raw file: {file_name}.')    
        logger.error("Error: {}".format(str(e)))
        

    try:
        print(f'Uploading raw file: {file_name}\nTo bucket/location: {bucket}/{object_key}')
        s3.put_object(Bucket=bucket, Key=object_key, Body=csv_file_zipped, ACL="bucket-owner-full-control")
        print('Upload successful.')
    except Exception as e:  
        print(f'Unable to upload raw file: {file_name} to s3 location: {object_key}.')      
        logger.error("Error: {}".format(str(e)))        


def write_to_s3(s3, csv_file, project, suffix, bucket, date):
    '''
    Writes the parsed version (changed position of long and lat in geometry) of the csv to s3. Gzips the file.
    '''
    
    unix_stamp = current_millisecond_time()
    
    print(f'Preparing to upload parsed version of {project}:  {project_suffix}')
    file_name = f'table.{project}_{project_suffix}.{unix_stamp}.batch.{unix_stamp}.fullscanned.true.csv.gz'
    object_key = f'{project}/{suffix}/{date}/{file_name}'

    try:
        print("Gzipping the object")
        csv_file_string = csv_file.getvalue() #Get string value from the csv_file StringIO object
        csv_file_bytes = csv_file_string.encode('utf8') #Encode the string value
        csv_file_zipped = gzip.compress(csv_file_bytes)
    except Exception as e:
        print(f'Unable to gzip file: {file_name}.')
        logger.error("Error: {}".format(str(e)))
        

    try:
        print(f'Uploading file: {file_name}\nTo bucket/location: {bucket}/{object_key}')
        s3.put_object(Bucket=bucket, Key=object_key, Body=csv_file_zipped, ACL="bucket-owner-full-control")
        print('Upload successful.')
    except Exception as e:   
        print(f'Unable to upload file: {file_name} to s3 location: {object_key}.')     
        logger.error("Error: {}".format(str(e)))
        

def lambda_handler(events, handler):
    time_start = time.time()
    status, url = get_secret(secretname)

    if not status:
        print("Error: Secrets configuration error.")
        return 
    
    try:
        print('Requesting data from the source API.')
        request = requests.get(url=url)
        #print(f'Response: {request} from url: {url}')
        data = request.text
        #print(data)
    except Exception as e:
        print(f'An error occurred while requesting data from api url: {url}.')     
        logger.error("Error: {}".format(str(e))) 
    
    date_current = current_date()
    s3 = boto3.client('s3')

    #Upload raw to landing bucket
    write_raw_to_s3(s3, data, project, project_suffix, landing_bucket, date_current)
    
    #Parse + upload parsed
    modified_csv = csv_parse(data)
    write_to_s3(s3, modified_csv, project, project_suffix, output_bucket, date_current)

    time_end = time.time() - time_start
    print(f'{project}: {project_suffix} successfully loaded. Load time {time_end} seconds')
    