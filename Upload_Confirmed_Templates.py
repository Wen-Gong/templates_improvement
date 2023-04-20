import json
from typing import TextIO
import numpy as np
import pandas as pd
import json
import boto3
from Functions.validation import *
from Functions.Update_template import *

Variables = {
        'ZipFile_backup': "backup/current_version.zip",
        'current_version_zipFile':'current_version.zip',
        'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
        'TableName_IMU': 'dailyimu',
        'DataBase': 'dailyimureportdb',
        'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
    }

def handler():
    #get confirmed templates for uploading
    data_from_json: TextIO = open("./confirmed_templates.json")
    confirmed_templates_dict: list = json.load(data_from_json)
    len(confirmed_templates_dict)

    #first change predicted_label

    confirmed_template_rawdata_list=[]
    for confirmed_template in confirmed_templates_dict:
        confirmed_template_rawdata_list.append(change_predicted_label(confirmed_template))

    #get new old version string
    new_version,old_version,new_version_file,old_version_file = get_latest_version_of_template_file(Variables['S3BucketName'])

    # move old template file to backup/current_version.zip
    move_zip_file_to_another(Variables['S3BucketName'], old_version_file,Variables['ZipFile_backup'])

    # check if old template and backup/current_version.zip are similar
    check_the_similarity_of_two_zip_files(Variables['S3BucketName'], old_version_file,Variables['ZipFile_backup'])

    clean_tmp_file()

    #upload additional templates to new zip file
    upload_new_templates_file_to_S3(Variables['S3BucketName'], old_version_file, confirmed_template_rawdata_list,new_version_file)

    # move new version zip file to current version
    move_zip_file_to_another(Variables['S3BucketName'], new_version_file,Variables['current_version_zipFile'])

    # check if new version file and current_version.zip are similar
    check_the_similarity_of_two_zip_files(Variables['S3BucketName'], new_version_file,Variables['current_version_zipFile'])