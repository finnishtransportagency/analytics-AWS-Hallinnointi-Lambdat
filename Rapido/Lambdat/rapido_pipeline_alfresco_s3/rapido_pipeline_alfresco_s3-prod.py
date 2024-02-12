import json
import os
import requests
from requests.auth import HTTPBasicAuth
import boto3
import botocore
from botocore.exceptions import ClientError
from datetime import datetime as dt
import datetime
import time


 
secretname = os.environ['secretname']
#Parent folder id. The excel file id will be requested with this
parent_folder_id = ''
#Excel file id. Its binary data will be requested from api with this
excel_id = ''
#The file itself converted from binary to .xlsx
excel_file = ''

output_bucket = os.environ['output_bucket']
output_folder = os.environ['output_folder']
#output_folder = 'testi'
line_break = '\r\n'
#parameters for api call
isFile_check = 'where=(isFile=true)'
isFolder_check = 'where=(isFolder=true)'
total_files = 0 #Total files processed for the success-print for quick overview of the run
excluded_errors = os.environ['excluded_errors'] #Temp solution to avoid errors from purposely empty folders in prod
excluded_errors = excluded_errors.split(';')


def get_secret(secretname):
    region_name = 'eu-central-1'
    status = False
    username = None
    password = None
    url = None    
    taustaryhmat_folder_id = None
    nested_folder_list = None
    excel_name_list = None
    folder_name_list = None

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
            #password = secret['password_dev']
            password = secret['password_prod']
            #url = secret['url_dev']            
            url = secret['url_prod']
            #taustaryhmat_folder_id = secret['taustaryhmat_folder_id_dev']
            taustaryhmat_folder_id = secret['taustaryhmat_folder_id_prod']
            nested_folder_list = secret['nested_folder_list'] #Actually ended up being a variable instead of a list for now
            excel_name_list = secret['excel_name_list']
            folder_name_list = secret['folder_name_list']
        
        
        status = True
    except json.JSONDecodeError as e:
        print('Error. Unable to convert retrieved secret string into json: {}'.format(e))
    except Exception as e:
        print("Error. Secret retrieve error: {}".format(e))

        

    return status, username, password, url, taustaryhmat_folder_id, nested_folder_list, excel_name_list, folder_name_list


def process_master_excel(username:str, password:str, api_url:str, id_name:tuple, modified_at:str):
    '''
    Processes a lonely master excel that is inside a folder that was expected to contain only folders.
    Fetches binary and saves it to s3.
    '''    
    
    #Use the excel ids to get the binary of each excel file
    excel_binary = get_excel_binary_content(username, password, api_url, id_name)
    #print(f'Master excel binary: {excel_binary}')
          
    #Save the binary as .xlsx into s3 bucket
    save_excel_to_s3(excel_binary, id_name, modified_at)
    

    global total_files
    total_files += 1
    
    



def get_all_folder_id_name(username:str, password:str, api_url:str, folder_id:str, folder_name_list: list, *excel_name_list:list):
    '''
    Gets all folder ids and names from folders that contain folders instead of excels.
    Those ids are used to get excel id's from nested folders.
    Also picks up and saves master excel, which is stored separately in a folder, without having its own folder. Quick solution at the end of project.
    For this reason, the isFolder_check can not be used here.
    '''

    url = f'{api_url}{folder_id}/children'
    #response is metadata of the folders
    response = None
    data = None
    folder_id_name_list = None
    #print(url)
    #print(folder_id)
    
    #if status:
    try:
        print('Initiating API call to request folder data on main or nested folder {}'.format(folder_id))
        #THIS GETS FOLDERS WITH FOLDERS
        response = requests.get(url, auth = HTTPBasicAuth(username, password))        
        print(response.content)
        
    except requests.exceptions.RequestException as e:
        print("Error. Unable to reach API for folder data from folder id: {} at URL: {}.{}Error {}".format(folder_id, url, line_break, e))

    try:
        data = json.loads(response.content)
    except json.JSONDecodeError as e:
        print(f"Error. Unable to convert retrieved excel data from folder '{folder_id}' into json: {e}")
        print(f'Data: {data}')        
        return folder_id_name_list
        
    #print("Content of the api call from url {}:".format(url))
    #print(data)

    #The json object where the items we want to read are at
    entries = data['list']['entries'] 
    folder_id_name_list = []

    data_count = data['list']['pagination']['count']        
         
    if int(data_count) == 0:
        print("Error. No items to get from folder (check documentation for folder name for the id, most likely 'Taustaryhmät'): {}".format(folder_id))        
        return folder_id_name_list
    
    for item in entries:
        #Check if the response returned content in folder:
        

        item_content = item['entry']
        is_folder = item_content['isFolder']
        is_file = item_content['isFile']
        item_name = item_content['name']
        item_id = item_content['id']
         
        
        if is_folder == False:            
            #Master excel is processed separately here, first, as a master should be
            if item_name in excel_name_list and is_file == True:
                
                created_at_raw = item_content['createdAt']
                modified_at_raw = item_content.get('modifiedAt', created_at_raw)            
                modified_at = modified_at_raw.split('T')[0]
                process_master_excel(username, password, api_url, (item_id,item_name), modified_at)
            continue
        
        
        if item_name in folder_name_list:            
            folder_id_name_tuple = (item_id, item_name)
            folder_id_name_list.append(folder_id_name_tuple)

    #print(f'List of folder id and names: {folder_id_name_list}')
    
    return folder_id_name_list


def get_excel_id_and_name(username: str, password: str, api_url: str, folder_id_name_tuple: tuple, excel_name_list: list, id_list: list):
    '''
    Gets an excel id from the folder and name the value of modifiedAt
    The id is used to get the excel binary.    
    '''    
    
    folder_id = folder_id_name_tuple[0]
    folder_name = folder_id_name_tuple[1]

    url = f'{api_url}{folder_id}/children?{isFile_check}'
    #response is metadata of the folder    
    response = None
    data = None
    excel_id = None
    excel_name = None
    modified_at = None
    
    
    try:
        print('Initiating API call to request excel metadata from folder {}, id: {}'.format(folder_name, folder_id))
        #THIS GETS FOLDERS WITH EXCELS
        response = requests.get(url, auth = HTTPBasicAuth(username, password))       
    except requests.exceptions.RequestException as e:
        print("Error. Unable to reach API for parent folder: {} id: {} at URL: {}.{}Error {}".format(folder_name, folder_id, url, line_break, e))

    try:
        data = json.loads(response.content)
    except json.JSONDecodeError as e:
        print(f"Error. Unable to convert retrieved excel data from folder '{folder_name}' into json: {e}")
        print(f'Data: {data}')
        return (excel_id, excel_name), modified_at
        
    #print("Content of the api call from url {}:".format(url))
    print(data)


    #The json object where the items we want to read are at
    entries = data['list']['entries']
    data_count = data['list']['pagination']['count']
    #print(f"Data count: {data_count}")
    

    if int(data_count) == 0:
        #temp solution for purposely missing data on prod
        if folder_name not in excluded_errors:
        #print(f'Excluded errors: {excluded_errors}')
            print("Error. No items to get from folder: {}, id: {}".format(folder_name, folder_id))            
        return (excel_id, excel_name), modified_at, 0
    
    for item in entries:
        #Check if the response returned content in folder:        
    
        item_content = item['entry']
        excel_name = item_content['name']
        excel_id = item_content['id']
        #is_file = item_content['isFile']        
        #print('Excel id = {}'.format(item))
        #print('Excel folder content = {}'.format(item_content))        
        #data_read_counter += 1        
        if excel_id in id_list:
            continue

        print(f'Checking if {excel_name} in excel name list..')
        if excel_name in excel_name_list:
            print("Check passed!")
            excel_id = item_content['id']     
            #print(excel_id)       
            created_at_raw = item_content['createdAt']
            modified_at_raw = item_content.get('modifiedAt', created_at_raw)            
            modified_at = modified_at_raw.split('T')[0]
            #print(modified_at)
            #data_read_counter += 1
            return (excel_id, excel_name), modified_at, data_count
        
                        
    
    #If the correct file is not found from this folder, return None and move on to next folder after this function
    return (excel_id, None), modified_at, 0
    


def get_excel_binary_content(username:str, password:str, api_url:str, id_name_excel:tuple):
    '''
    Gets an excel binary with an excel file id.
    '''

    excel_id = id_name_excel[0]
    excel_name = id_name_excel[1]
    
    url = f'{api_url}{excel_id}/content'
    #response is the binary of the excel file
    response = None
    excel_binary = None

    
    try:
        print('Initiating API call to request the binary of the excel: {}'.format(excel_name))
        response = requests.get(url, auth = HTTPBasicAuth(username, password))
        excel_binary = response.content
        #print(f'Excel binary of {excel_name}: {excel_binary}')
    except requests.exceptions.RequestException as e:
        print("Unable to reach API for excel id: {}, name: {} at URL: {}. Error {}".format(excel_id, excel_name, url, e))
        
    
    #print("Binary content of the excel from url {}:".format(url))
    #print(excel_binary)
    return excel_binary
    
    
def save_excel_to_s3(excel_file_binary:bytes, id_and_name:tuple, modified_at:str):
    '''
    Saves the excel binary to s3.
    '''

    #Handles the .xlsx suffix to correct place in final name
    split_name = id_and_name[1].split('.')
    file_name = split_name[0].replace('ä', 'a').replace('ö', 'o').replace('å', 'a').replace(' ', '_').replace('-', '_').replace('&', '_')
    file_suffix = split_name[1]
    s3_key = f'{output_folder}/{file_name}_{modified_at}.{file_suffix}'
    s3 = boto3.client('s3')

    
    try:
        print("Saving binary of {} as excel to s3 as {}".format(file_name, s3_key))
        s3.put_object(Body=excel_file_binary, Bucket=output_bucket, Key=s3_key)
        print("Save OK.")
    except ClientError as e:
        msg = e.response['Error']['Message']
        print(f"ClientError: {e},{line_break}Error message: {msg}{line_break}while trying to save {s3_key} to bucket: {output_bucket} ")
        #print('Failed to upload file "{}" to s3 bucket "{}".'.format(file_name, output_bucket))
    except Exception as e:
        print(f"An error occurred while saving file: {s3_key} to bucket: {output_bucket}.{line_break}Error: {e}")
    

def lambda_handler(event, context):
    '''
    This is the main function that orchestrates everything.
    '''

    print("Initiating pipeline, fetching secrets.")
    timer_start = time.time()
    #Total number of files fetched. It should be 19 at the last print, once the production is fully completed (17 as of 23.8.2023)
    global total_files  

    status, username, password, url, taustaryhmat_folder_id, nested_folder_list, excel_name_list, folder_name_list = get_secret(secretname)
    excel_name_list = excel_name_list.split(';')
    folder_name_list = folder_name_list.split(';')
    '''Use when/if variable needs more than 1 folders:
    nested_folder_list = nested_folder_list.split(';')'''
    
    if status:
        parent_folder_id_name_list = get_all_folder_id_name(username, password, url, taustaryhmat_folder_id, folder_name_list, *excel_name_list)
        if parent_folder_id_name_list == None:
            print('Error. Fatal. Parent folder returned "None". Check the API Data if it is empty.')
    
    #Loop through the folder id's
    for parent_folder_tuple in parent_folder_id_name_list: 
        #Variable names for normal, non-nested, folders:
        folder_id = parent_folder_tuple[0]
        folder_name = parent_folder_tuple[1]
        

        read_counter = 0
        count_of_data = 1
        excel_id_checked = []
        
        #Currently only one nested folder. If we get more, change nested_folder_list into actual list and split it after get_secret()
        #Process nested folder:
        if folder_name == nested_folder_list:
            print("Processing nested folder: {}".format(folder_name))
            nested_folder_id_name_list = get_all_folder_id_name(username, password, url, folder_id, folder_name_list)
            if nested_folder_id_name_list == None:
                continue

            for folder_tuple in nested_folder_id_name_list:
                #Variable names for nested folder:
                folder_name_n = folder_tuple[1]
                folder_id_n = folder_tuple[0]
                #Use the folder ids to get excel id's from within those folders
                excel_id_and_name, date_modified, count_of_data = get_excel_id_and_name(username, password, url, folder_tuple, excel_name_list, excel_id_checked)
                print(f'excel_id_and_name: {excel_id_and_name} and {date_modified}')

                if excel_id_and_name == (None, None) and date_modified == None: 
                    #Below if is temporary
                    if folder_name_n not in excluded_errors:
                        print(f"Error. An error occured while returning to main function with None values, or nothing to process from folder: {folder_name_n} with id: {folder_id_n}. Moving onto next folder id on the list or finishing run if this was last id.")
                    continue

                #Use the excel ids to get the binary of each excel file
                excel_binary = get_excel_binary_content(username, password, url, excel_id_and_name)        
                #Save the binary as .xlsx into s3 bucket
                save_excel_to_s3(excel_binary, excel_id_and_name, date_modified)
               
                
                total_files += 1

            print("Ending processing of nested folder: {}".format(folder_name))
            
            


        #If folder contains excel files, process normally:
        else:
            
            while read_counter < count_of_data:
                #Use the folder ids to get excel id's from within those folders
                excel_id_and_name, date_modified, count_of_data = get_excel_id_and_name(username, password, url, parent_folder_tuple, excel_name_list, excel_id_checked)
                read_counter += 1

                print(f'excel_id_and_name: {excel_id_and_name} and {date_modified}')
                excel_file_id = excel_id_and_name[0]
                excel_file_name = excel_id_and_name[1]

                excel_id_checked.append(excel_file_id)

                if excel_id_and_name == (None, None) and date_modified == None:
                    #Below if is temporary
                    if folder_name not in excluded_errors:
                        print(f"Error. An error occured while returning to main function with None values, or nothing to process from folder: {folder_name} with id: {folder_id}. Moving onto next folder id on the list or finishing run if this was last id.")
                    continue

                if excel_file_name not in excel_name_list:
                    print(f'File named {excel_file_name} not found in excel_name_list. Please check if it should be there (most likely not as this is to filter out unwanted, non error items.)')
                    continue

                #Use the excel ids to get the binary of each excel file
                excel_binary = get_excel_binary_content(username, password, url, excel_id_and_name)
                #Save the binary as .xlsx into s3 bucket
                save_excel_to_s3(excel_binary, excel_id_and_name, date_modified)   

                if read_counter < count_of_data:    
                    print(f'More files to check from folder {folder_name}. Looping the folder again to look for downloadable files.')     

                elif read_counter >= count_of_data:
                    print(f'All files from folder {folder_name} downloaded. Moving on to the next one')



            total_files += 1    
    

    timer_end = time.time()
    total_time = int(timer_end-timer_start)
    minutes = total_time//60
    seconds = total_time%60
    print("A total of {} files processed in: {} minutes, {} seconds. Total seconds: {}".format(total_files, minutes, seconds, total_time))
    

