"""
Suoraan "vanhaan paikkaan" kijoitettava polku:
Target bucket: 
file-load-ade-runtime-dev
Polku:
sampo_vt_tuote2013_obs/
table.sampo_vt_tuote2013_obs.1671543041250.batch.1671543041250.fullscanned.true.csv.gz

--

Suoraan "uuteen paikkaan" kirjoitettava polku
Target bucket: 
vayla-file-load-bucket-dev
Polku:
sampo/
vt_tuote2013_obs/
2023/
01/
05/
table.sampo_vt_tuote2013_obs.1672942959052.batch.1672942959052.fullscanned.true.csv.gz


"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import json
import csv
import base64
from botocore.exceptions import ClientError
import requests
#from botocore.vendored import requests
import boto3
import time
import datetime
import os
import gzip

s3 = boto3.resource('s3')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'secretname', 'db_tables', 'prod_trgt_bucket'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Variables placed manually. 

extract_bucket = 'iamanalytics-landing-bucket-prod' # On this account MUUTTAA
source_bucket = 'iamanalytics-landing-bucket-prod' #MUUTTAA
#dev_target_bucket = 'talous-sampo-ade-target-test' # On sandbox account
#dev_target_bucket ='aaa-oikeustestibuketti'
#dev_target_bucket ='vayla-file-load-bucket-dev'
#prod_target_bucket = 'vayla-file-load-bucket-prod'
#
#
prefix = 'iamanalytics/'  #MUUTTAA
#
#

def get_secret(secretname):

   
    region_name = 'eu-central-1'
    username = None
    password = None
    host = None
    dbname = None

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secretname 
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        username = secret['username']
        password = secret['password']
        host = secret['host']
        dbname = secret['dbname']
	    #dburl = 'jdbc:oracle:thin:@//'+secret['host']+':'+secret['host']+'/'+secret['dbname']

    return username, password, host, dbname

def current_milli_time():
    """ Generates timestamp strings.
    Returns:
        Current time in milliseconds.
    """
    return str(int(round(time.time() * 1000)))
    
def current_date_path():
    """
    Returns a date as string like:
    2022/12/16
    """
    dt = datetime.datetime.now()
    #return str(dt.year)+'/'+str(dt.month)+'/'+str(dt.day)+'/'
    return dt.strftime('%Y')+'/'+dt.strftime('%m')+'/'+dt.strftime('%d')+'/'


def get_export_file_name(import_file_name):  # File: talous-sampo-data/vt_liikenne12_obs/part-00000-b3326108-86dd-4317-a2e4-c0c1440fbe80-c000.csv.gz
    """ Generates a name  based on the ADE naming standards.
    Example input:
        "sourcetable"
    Generates following output:
        "project_sourcetable/table.project_sourcetable.year.2018-2019.1611251680338.batch.1234567.fullscanned.true.delim.comma.skiph.1.csv"
    
    Args:
        import_file_name: File name to be reformatted.
    Returns:
        File name in the ADE format.
    """
    timestamp = current_milli_time()
    
    file_name_new=import_file_name.split("/")[1]

    #folder_name = "sampo_" + file_name_new
    folder_name = file_name_new
	 
    
    # Parameters for ADE are given in the file name.
    # Details: https://intra.solita.fi/display/DATA/ADE+File+loading+specification
    file_suffix = f'.{timestamp}.batch.{timestamp}.fullscanned.true.csv.gz'
    file_prefix = "table.iamanalytics_" #MUUTTAA
    
    #file_name = file_prefix + project_name + import_file_name + file_suffix
    file_name = file_prefix + file_name_new + file_suffix
    
    
    #
    #Testing creating a path according to the current date
    
    datepath = current_date_path()
    file_prefix_w_dtpath = datepath + file_prefix
    added_file_name = file_prefix_w_dtpath + file_name_new + file_suffix
    export_file_name = folder_name + "/" + added_file_name
    #print("Testing date path: " + str(export_file_name))
    #
    # Seems to be ok.
    #
    # Logging
    #print("Exporting file to: " + str(export_file_name))

    return export_file_name





def fetch_filenames_s3(bucket, prefix):    # (source_bucket, prefix) = (sillaritest, 'talous-sampo-data/')
    
    s3 = boto3.client('s3')
    keys = []

    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    #print("IMPORT PATH: {}".format(prefix))  #talous-sampo-data/vt_liikenne12_obs/part-00000-b3326108-86dd-4317-a2e4-c0c1440fbe80-c000.csv.gz, ....
    #print(keys)
    return keys


	
def copy_files_prod(bucket, filenames):
    """ Downloads a file content from a S3 bucket.

    Args:
        - bucket: source (import) S3 bucket.
        - filename: Name of the file in the bucket.
		- - prod_target_bucket: where to -bucket

    """
    
    s3_client = boto3.client('s3')
    
    prod_target_bucket=args['prod_trgt_bucket']
    for file in filenames:
        """
        print("Import bucket: {}".format(bucket))
        print("Where to -bucket: {}".format(prod_target_bucket))
        print("File: {}".format(file))
        new_filename=get_export_file_name(file)
        print("New filename: {}".format(new_filename))
        """
        
        try:
            print("PROCESSING: \"{}\"".format(str(file))) #talous-sampo-data/vt_liikenne12_obs/part-00000-b3326108-86dd-4317-a2e4-c0c1440fbe80-c000.csv.gz
            filename_w_path = get_export_file_name(file)
            
            print("Where to copy -bucket: {}".format(prod_target_bucket))
            #print("New filename with path: {}".format(filename_w_path))
            
            #new_filename=filename_w_path.split("/")[1]
            
            # For a test
            folder_and_new_filename = prefix + filename_w_path
            
            #folder_and_new_filename = filename_w_path
            
            print("New folder and file: {}".format(folder_and_new_filename))
            copy_src = {'Bucket': bucket, 'Key': file}
            s3_client.copy_object(ACL='bucket-owner-full-control', CopySource=copy_src, Bucket=prod_target_bucket, Key=folder_and_new_filename)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print("File \"{}\" not found in bucket \"{}\".".format(str(file), str(bucket)))
            else:
                raise e
               
    
    return ("prod files copied!")
	
def read_data_from_sourcedb():
    username, password, host, dbname = get_secret(args['secretname'])
    #
    tables=args['db_tables']
    #
    # Construct JDBC connection options
    for table in tables.split(','):
        connection_sqlserver_options = {
        "url": "jdbc:sqlserver://"+host+":3044/"+dbname,
        "dbtable": table,
        "user": username,
        "password": password,
        #"customJdbcDriverS3Path": "s3://talous-sampo-ade-landingbucket/oracle-driver/ojdbc8.jar",
        #"customJdbcDriverClassName": "oracle.jdbc.OracleDriver"
        }
        # Read DynamicFrame from Oracle
        df_sqlserver = glueContext.create_dynamic_frame.from_options(connection_type="sqlserver", connection_options=connection_sqlserver_options)
        #    
        #print("rows = {}".format(df_oracle8.count()))  
        df=df_sqlserver.toDF()
        cols=df.columns
        #	
        #print(cols)
        #df.select(df.columns[:3]).show(3)
        #
        #df.printSchema()
        #
        #types = [f.dataType for f in df.schema.fields]
        #print("rows = {}".format(types[0])) 
        #
        sql = 'select ' + ','.join(["regexp_replace(" + str(val) +",'[\|\\n\\r]+',' ') as " + str(val) for val in cols]) + " from data_table"
        print(sql)
        df2=df.registerTempTable("data_table")
        folder=table.split(".")[1]
        df_sql=spark.sql(sql).coalesce(1).write.format("csv").mode("overwrite").option("delimiter",";").option("quoteAll","True").option("escape",'"').option("header","False").option("codec","gzip").save("s3a://%s/iamanalytics/%s" % (extract_bucket, folder))
    return ("Source data copied!")
#
dbconnect_ok=read_data_from_sourcedb()
print(dbconnect_ok)
filenames=fetch_filenames_s3(source_bucket, prefix)
ok_prod=copy_files_prod(source_bucket, filenames)
print(ok_prod)
#ok_dev=copy_files_dev(source_bucket, filenames)
#print(ok_dev)
job.commit()
