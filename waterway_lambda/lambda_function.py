import os
import requests
import json
import boto3
from requests.auth import HTTPBasicAuth
import datetime
import time


#TODO Vaihda lambdan nimi väyläpilveen -> ais-waterway-api-s3-csv-pipeline

def get_secret(secretname):
    region_name = 'eu-central-1'
    status = False
    username = None
    password = None
    url = None
    test_password = None
    test_url = None

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
            username = secret['username']
            password = secret['password']
            url = secret['url']
            test_password = secret['test_password']
            test_url = secret['test_url']
        
        #return username, password, url
        status = True
    except Exception as e:
        print("Secret {} retrieve error: {}".format(secretname, e))
        

    return status, username, password, url, test_password, test_url


def current_millisecond_time():
    #Generates current epoch time in milliseconds.    
    return str(int(round(time.time() * 1000)))
        

def current_date():
    #Returns current date as a string (YYYY/MM/DD):    
    dt = datetime.datetime.now()
    time = dt.strftime("%Y/%m/%d")
    return time #Palauttaa yksinhumeroiset arvot kaksinumeroisina



"""
datasetti_lista = os.environ["Dataset"].split(",")






def assume_role():
    try:
        sts_connection = boto3.client('sts')
        acct_b = sts_connection.assume_role(
            RoleArn="arn:aws:iam::426182641979:role/waterway_crossaccount_role",
            RoleSessionName="cross_acct_lambda"
        )
    except Exception as e:
        print("sts connection for boto3.client failed for assume_role()")

    ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
    SECRET_KEY = acct_b['Credentials']['SecretAccessKey']
    SESSION_TOKEN = acct_b['Credentials']['SessionToken']

    # create service client using the assumed role credentials:
    try:
        client = boto3.resource(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
    )
    except Exception as e:
        print("Failed to create boto3.client for assume_role()")

    
    return client
    
"""

def s3_write(bucket_name, file_name, data):
    """
    Writes all files to s3 bucket vayla-file-load in both dev and prod environments
    """
    s3 = boto3.resource('s3')
    
    try:
        s3.Bucket(bucket_name).put_object(Key=file_name, Body=data, ACL='bucket-owner-full-control')
        obj_length = len(data)
        print("Wrote file: {} to bucket: {}. File length: {}.".format(file_name, bucket_name, obj_length))        
    except Exception as e:
        print(f"Failed to write {file_name} to bucket {bucket_name}. Error: {e}")
    


def header_parse(header, data, id_text):
    """
    Parses the given json object to create headers for the file. Strips \n, \r and changes " -> "".
    """
    if data != None and header == '':                            
        if len(data) > 0:
            header = '"{}"'.format(id_text)
            objekti = data[0]                           
            for vfield in objekti:

                vfield = str(vfield).replace("\n", " ").replace("\r", " ").strip().lower()
                header += ';"' + vfield + '"'
            header += "\n"
    
    return header

def data_parse(data, parent_id):
    """
    Parses the given json data into csv-format within a variable.
    """
    result = ''
    quote = "\""
    escape = "\""
    if data == None:
        return result
    for obj in data:
        wayline = '"' + parent_id + '"'
        
        for xfield in obj:
            wv = obj[xfield]                                                              
            if wv == None:
                wv = ''
            wv = str(wv).replace("\n", " ").replace("\r", " ").replace(quote, quote + escape).strip()
            
            wayline += ';"' + wv + '"'

        result += wayline + "\n"
    return result        
            


def lambda_handler(events, context):
    """
    The main function that orchestrates the lambdas process. Contains parts of the transform process that isnt included in the other functions.
    """
    time_track = time.time()
    secretname = os.environ["secretname"]
    status, username, password, url, test_password, test_url = get_secret(secretname)
    
    #Double checks if secrets were retreived successfully
    if not status:
        return "Secrets configuration error"

    target_bucket = os.environ["target_bucket"]
    ais_landing_bucket = os.environ['ais_bucket'] #This is ais landing bucket for copy of vaylaalue (fairway areas) file for container geoPointsCompare
    filter = os.environ["filter"]
    # HUOM: jos tekee yhden arvon jossa esim , erotettu lista niin voi muuttaa lennosta (lähinnä poisto/yhden testaus kerrallaan)
    
    """
    dataset1 = os.environ["dataset1"]
    dataset2 = os.environ["dataset2"]
    dataset3 = os.environ["dataset3"]
    
    dataset_list = [dataset1, dataset2, dataset3]
    dataset_list_checked = []
    for i in dataset_list:
        if len (i)>2 and i != '':
            dataset_list_checked.append(i)
    """        
            
    dataset_list = []
    #checks for allt he dataset names found within environmental variables. Range can be limited for testing purposes.
    print("Checking available datasets from environment variables")
    for i in range(1,4):
        
        id = "dataset{}".format(i)
        try:
            
            if id in os.environ:
                v = os.environ[id]
                if v != '':
                    dataset_list.append(v)
        except Exception as e:
            print("Dataset {} not found. Error {}".format(id, e))
            continue
    print("Found {} datasets: {}".format(len(dataset_list), dataset_list))
    if len(dataset_list) < 1:
        print("Nothing to process. No datasets were found for processing.")
        
        
    
    #varmistaa, että urlin muoto on oikea, https://api.fi/rajapinta/data?x=1,2
    if "/" not in url[-1]:
            url += "/"
    if "?" not in filter[0]:
        filter = "?"+filter
    
    for datasetti in dataset_list:

        if datasetti == '':
            continue
        
        
        print("Fetching data from API for dataset: {}".format(datasetti))
        url_get	= f"{url}{datasetti}{filter}" #.format(url, datasetti, url_end) 
        response = None
        
        try:
            response = requests.get(url_get, auth = HTTPBasicAuth(username, password))
            #print("API fetch successful")
        except requests.exceptions.RequestException as e:
            print("Unable to reach API for dataset: {} at URL: {}. Error {}".format(datasetti, url_get, e))
            continue
        if response.status_code == 401:
            print("API respose 401. Unable to access data due to access denied. Shutting down operation.")
            
       
        
        json_data = ''
        try:
            json_data = json.loads(response.text)
        except Exception as e:
            print("Json.loads failed for dataset {}. Error: {}".format(datasetti, e))
    
    
        # Parserin testi, luetaan tallennettu json levyltä
        #f = open(ident + "_kaikki_response.text.txt", "r")
        #data = json.loads(f.read())
        #f.close()
    
    
    
        header = ''
        vaylaheader = ''
        luokitusheader = ''
        mitoitusheader = ''
    
        data = ''
        vayladata = ''
        luokitusdata = ''
        mitoitusalusdata = ''
        
        quote = "\""
        escape = "\""
        
        
    
        if len(json_data) > 0:
    
            fields = []
            header = ''
            
            count = 0
            
            #Loops through the json and fetches the field names 
            for item in json_data[0]:
                fields.append(item)
                if count > 0:
                    header += ';'
                count += 1
                item = str(item).replace("\n", " ").replace("\r", " ").strip().lower()
                header += '"' + item + '"'
    
            #af.write(header + '\n')
            data += header + '\n'
    
            wayrowcounter = 0
            rowcounter = 0
            processed = []
            
            #Loops the json and determines the name of the 'id' field
            print("Starting to process dataset: {}. No of items = {}".format(datasetti, len(json_data)))
            for item in json_data:
                rowcounter += 1
    
                line = ''
                colcounter = 0
                
                if datasetti == "vaylat":
                    dataid = str(item['jnro'])
                else:
                    dataid = str(item['id'])

                if dataid not in processed:
                    processed.append(dataid)
                else:
                    continue

                processed_vayla = [] #tarkistaa ettei sama väylä tule kahdesti 
                processed_luokitus = []
                processed_mitoitusalus = []
                
                #Loops the field names in fields-list and parses the headers and data into the csv-like variables.
                
                for field in fields:
                    v = ''
                    
                        
                    
                    if field == 'vayla':
                        
                        #varmistetaan, ettei tule sama vayla kahdesti.
                        if dataid not in processed_vayla:
                            #print("Starting to process vayla-field for dataset: {}".format(datasetti))
                            processed_vayla.append(dataid)
                        
                        
                        if vaylaheader == '':
                            #tässä luodaan otsikko tiedostolle
                            if datasetti == "vaylaalueet":                                                              
                                vaylaheader = header_parse(vaylaheader, item[field], "vaylaalueid") 
                            elif datasetti == "navigointilinjat":                                
                                vaylaheader = header_parse(vaylaheader, item[field], "navigointilinjaid") 
                            vayladata += vaylaheader
                            #parsitaan kentän data
                        v = data_parse(item[field], dataid)  
                        if v != '':                        
                            vayladata += v
                        v = ''

                    elif field == 'luokitus':
                        
                        if dataid not in processed_luokitus:
                            #print("Starting to process luokitus-field for dataset: {}".format(datasetti))
                            processed_luokitus.append(dataid)
                        
                          
                        if luokitusheader == '':                              
                            luokitusheader = header_parse(luokitusheader, item[field], "vaylaid") 
                            luokitusdata += luokitusheader
                        v = data_parse(item[field], dataid)  
                        if v != '':                            
                            luokitusdata += v
                        v = ''

                    elif field == 'mitoitusalus':
                        
                        if dataid not in processed_mitoitusalus:
                            #print("Starting to process mitoitusalus-field for dataset: {}".format(datasetti))
                            processed_mitoitusalus.append(dataid)
                            
                        
                            
                        if mitoitusheader == '':
                            mitoitusheader = header_parse(mitoitusheader, item[field], "vaylaid")                        
                            mitoitusalusdata += mitoitusheader
                        v = data_parse(item[field], dataid)  
                        if v != '':
                            mitoitusalusdata += v           
                        v = ''        

                    #Kentän parsiminen, jos field ei ole 'vayla', ''luokitus' tai 'mitoitusalus'
                    
                    else:                        
                        v = item[field]
                        #print("Muu kuin väylä tai luokitus prosessoitu. {}".format(datasetti))
                    if v == None:
                        v = ''
                    if colcounter > 0:
                        line += ';'
                    v = str(v).replace("\n", " ").replace("\r", " ").replace(quote, quote + escape).strip()
                    line += '"' + v + '"'
                    colcounter += 1
                
                data += line + '\n'
                                   
    
        timestamp = current_date()
        epoch_current = current_millisecond_time()
        #Tallentaa kaikkien datasettien datan pääosan
        file_name = f"waterway/{datasetti}/{timestamp}/table.waterway_{datasetti}.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv"
        print("Files processed for dataset: {}. Starting to write files.".format(datasetti))
        try:
            s3_write(target_bucket, file_name, data)               
        except:
            print("Unable to write file {} to bucket {}".format(file_name, target_bucket))

        #This will save a copy of the vaylaalueet file to ais landing bucket for geoPointsCompare to consume
        if datasetti == 'vaylaalueet':
            file_name = "areas/ais-waterway-{}.csv".format(datasetti)
            
            try:
                s3_write(ais_landing_bucket, file_name, data)                
            except:
                print("Unable to write file {} to bucket {}. This affects the data for geoPointsCompare container but nothing else.".format(file_name, ais_landing_bucket))
        # Processes the saving of the variables into the files.
        
        """ with open(f"{ADE_bucket}{timestamp}{datasetti}.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv", "w") as f:
            f.write(data)  """
    #waterway/vaylaalueet/vuosi/kk/pvm/table.waterway_vaylaalueet.LOPPUOSA.csv
        #Nämä tallentavat datan "luokitus" tai "väylät" json-objektin
        if datasetti == "vaylat":
            
            epoch_current = current_millisecond_time()
            #Määrittää linkkitiedoston adenimen sen mukaan onko kyseessä _luokitus vai _mitoitusalus
            
            file_name = f"waterway/{datasetti}_luokitus/{timestamp}/table.waterway_{datasetti}_luokitus.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv"
            
            try:
                s3_write(target_bucket, file_name, luokitusdata)                  
            except:
                print("Unable to write file {} to bucket {}".format(file_name, target_bucket))
                
            file_name = f"waterway/{datasetti}_mitoitusalus/{timestamp}/table.waterway_{datasetti}_mitoitusalus.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv"
            
            try:
                s3_write(target_bucket, file_name, mitoitusalusdata)                
            except:
                print("Unable to write file {} to bucket {}".format(file_name, target_bucket))
            #s3.Bucket(bucket, adenimi_link)
            #object.put(Body=vayladata)
            #trystä oma metodi, file nimi, datamuuttuja

            
            """ with open(f"{ADE_bucket}{timestamp}{datasetti}_luokitus.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv", "w") as f:
                f.write(vayladata) """
        else:
            epoch_current = current_millisecond_time()
            file_name = f"waterway/{datasetti}_vayla/{timestamp}/table.waterway_{datasetti}_vayla.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv"
            
            try:
                s3_write(target_bucket, file_name, vayladata)
                print("Wrote file: {} to bucket: {} for dataset: {}".format(file_name, target_bucket, datasetti)) 
            except:
                print("Unable to write file {} to bucket {}".format(file_name, target_bucket))
            """ with open(f"{ADE_bucket}{timestamp}{datasetti}_vayla.{epoch_current}.batch.{epoch_current}.fullscanned.true.csv", "w") as f:
                f.write(vayladata) """
    
        print("Load completed successfully for dataset {}".format(datasetti))
    time_track_end = time.time()
    time_total = int(time_track_end - time_track)
    print("All datasets processed. Load time {} seconds".format(time_total))
    return
#lambda_handler(None, None)