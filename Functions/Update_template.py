import shutil
import os
import  pandas as pd
from loguru import logger
from typing import  *
import zipfile
import glob
import boto3
import numpy as np
import json
import zipfile
import time
import io

Variables = {
        'ZipFile': "templates_wen_v12.zip",
        'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
        'TableName_IMU': 'dailyimu',
        'DataBase': 'dailyimureportdb',
        'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
    }

#Use athena to query data, save to s3 bucket and then return the QueryExecutionId
def execute_query(query:str)-> str:

    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': Variables['DataBase']
        },
        ResultConfiguration={
            'OutputLocation':'s3://'+Variables['S3BucketNameSaveQuery']+'/',
        }
    )

    return response['QueryExecutionId']

#clean the temporary menmory of AWS virtual environment
def clean_tmp_file():
    shutil.rmtree('/tmp/')
    os.mkdir('/tmp/')

#get the rawdata of the templates from a zip file
def loadTemplatesFromS3(s3FileBucket, s3FileKey) -> [Dict, Dict]:
    logger.info(f'Loading templates from : {s3FileBucket} and {s3FileKey}')
    # Download templates ZIP file from S3
    s3_resource = boto3.resource("s3")
    s3_resource.Bucket(s3FileBucket).download_file(
        s3FileKey, "/tmp/templates.zip"
    )

    # Extract contents of zip file
    with zipfile.ZipFile("/tmp/templates.zip", "r") as f:
        f.extractall("/tmp/templates/")

    # Load all templates
    allTemplates = []

    cutoffs = {}
    fileNames = glob.glob("/tmp/templates/**/*.json", recursive=True)
    fileNames = sorted(fileNames)

    #print(fileNames)
    for f in fileNames:
        with open(f) as json_file:
            data = json.load(json_file)

            if "cutOff" in fileNames:
                cutoffs = data
            else:
                allTemplates.extend(data)

    logger.info(f'Loaded : {len(allTemplates)} templates')

    return allTemplates, cutoffs

#get gettemplateArrays and classification
def gettemplateArraysClassUUIDs(templates) -> [List[np.array], List[str], List[str]]:
    templateArrays = []
    templateClass = []
    templateUUIDs = []
    for template in templates:
        templateId = template['MetaData']['id']
        templateLabel = template['Instances']['prediction']['predicted_label']

        # load template Data
        templateData_temp = template['Instances']['features']['featurevector']
        df = pd.DataFrame(templateData_temp, columns=['ts', 'x', 'y', 'z'])
        del df['ts']
        #for col in df[['x','y','z']]:
        for col in df:
            df[col] = df[col].astype(float)
        templateData = df.values

        templateClass.append(templateLabel)
        templateArrays.append(templateData)
        templateUUIDs.append(templateId)

    return templateArrays, templateClass, templateUUIDs

# add new template(rawdata) to json with all current templates, and then return new json file
def add_new_template_to_zip_file(s3FileBucket, s3FileKey,new_template:Dict) -> [Dict, Dict]:
    logger.info(f'Loading templates from : {s3FileBucket} and {s3FileKey}')
    # Download templates ZIP file from S3
    s3_resource = boto3.resource('s3')
    s3_resource.Bucket(s3FileBucket).download_file(
        s3FileKey, "/tmp/templates.zip"
    )

    # Extract contents of zip file
    with zipfile.ZipFile("/tmp/templates.zip", "r") as f:
        f.extractall("/tmp/templates/")

    # Load all templates
    allTemplates = []
    cutoffs = {}
    fileNames = glob.glob("/tmp/templates/**/*.json", recursive=True)
    fileNames = sorted(fileNames)
    for f in fileNames:
        with open(f) as json_file:
            json_data = json.load(json_file)

    json_data.append(new_template)

    return json_data

# delete latest template(rawdata) to json with all current templates, and then return new json file
def delete_new_template_to_zip_file(s3FileBucket, s3FileKey) -> [Dict, Dict]:
    logger.info(f'Loading templates from : {s3FileBucket} and {s3FileKey}')
    # Download templates ZIP file from S3
    s3_resource = boto3.resource('s3')
    s3_resource.Bucket(s3FileBucket).download_file(
        s3FileKey, "/tmp/templates.zip"
    )

    # Extract contents of zip file
    with zipfile.ZipFile("/tmp/templates.zip", "r") as f:
        f.extractall("/tmp/templates/")

    # Load all templates
    allTemplates = []
    cutoffs = {}
    fileNames = glob.glob("/tmp/templates/**/*.json", recursive=True)
    fileNames = sorted(fileNames)
    for f in fileNames:
        with open(f) as json_file:
            json_data = json.load(json_file)
    json_data=json_data[:-1]

    return json_data

#get rawdata from csv file
def get_template_rawdata_from_athena(athena_query):
    s3 = boto3.resource('s3')
    potential_template_data_query_request_id = execute_query(athena_query)
    time.sleep(3)
    potential_template_data_csv = s3.Bucket(Variables['S3BucketNameSaveQuery']).Object(
        key=potential_template_data_query_request_id + '.csv').get()
    potential_template_rawdata = pd.read_csv(io.BytesIO(potential_template_data_csv['Body'].read()), encoding='utf8')[
        'rawdata']

    rawdata =json.loads(potential_template_rawdata[0])

    return rawdata

# upload the adjusted json file to zip file which located in the S3 (add one)
def upload_new_templates_file_to_S3(s3bucket:str, zipfilename:str, new_template_rawdata:json):
    s3_resource = boto3.resource("s3")
    new_json = add_new_template_to_zip_file(s3bucket, zipfilename, new_template_rawdata)
    with open("/tmp/allTemplates.json", "w+") as f:
        json.dump(new_json, f)
    handle = zipfile.ZipFile('/tmp/' + zipfilename, 'w')
    handle.write("/tmp/allTemplates.json", compress_type=zipfile.ZIP_DEFLATED)
    handle.close()

    s3_resource.meta.client.upload_file('/tmp/' + zipfilename, s3bucket, zipfilename)

# upload the adjusted json file to zip file which located in the S3 (delete one)
def delete_new_templates_file_to_S3(s3bucket:str, zipfilename: str):
    s3_resource = boto3.resource("s3")
    new_json = delete_new_template_to_zip_file(s3bucket, zipfilename)
    with open("/tmp/allTemplates.json", "w+") as f:
        json.dump(new_json, f)
    handle = zipfile.ZipFile('/tmp/' + zipfilename, 'w')
    handle.write("/tmp/allTemplates.json", compress_type=zipfile.ZIP_DEFLATED)
    handle.close()

    s3_resource.meta.client.upload_file('/tmp/' + zipfilename, s3bucket, zipfilename)

#original templates situation
#clean_tmp_file()

#current templates situation
clean_tmp_file()
#print(glob.glob("/tmp/*"))
#potential_template_data_query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                                 #"from " + Variables['TableName_IMU'] + " " \
                                 #"where  predictedlabel = 'harshBraking' " \

#template_rawdata = get_template_rawdata_from_athena(potential_template_data_query)
#upload_new_templates_file_to_S3(Variables['S3BucketName'], Variables['ZipFile'], template_rawdata)
#loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])
#loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])
#delete new template
#delete_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile'])
#loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])
#delete_new_templates_file_to_S3(Variables['S3BucketName'], Variables['ZipFile'])
#loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])