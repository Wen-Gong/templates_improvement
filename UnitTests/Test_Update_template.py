import unittest
import json
import numpy as np
from Functions.Update_template import clean_tmp_file,loadTemplatesFromS3,gettemplateArraysClassUUIDs, add_new_template_to_zip_file,get_template_rawdata_from_athena, delete_new_template_to_zip_file, get_template_rawdata_from_athena, upload_new_templates_file_to_S3,delete_new_templates_file_to_S3

class Test_loadTemplatesFromS3(unittest.TestCase):
    def setUp(self):
        self.variable = {
            'ZipFile': "templates_wen_v12.zip",
            'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
            'TableName_IMU': 'dailyimu',
            'DataBase': 'dailyimureportdb',
            'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
        }
    #test the number of templates while getting template file from s3
    def test_the_number_of_templates_in_zip(self):
        Variables = self.variable
        clean_tmp_file()
        result = len(loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0])
        print(loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0][0]['RequestId'])
        #def delete_specific_template(template_json_file_fronm_s3, RequestId):
            #for template_raw_data in template_json_file_fronm_s3:
               # if template_raw_data['RequestId']=='5499f15b-18d9-45f9-8756-b11fec616f59':

        expected= 240
        self.assertEqual(result, expected)

    # test the format of templates file while getting template file from s3
    def test_the_format_of_template_in_zip(self):
        Variables = self.variable
        clean_tmp_file()
        result = type(loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0][0])
        expected= dict
        self.assertEqual(result, expected)


class Test_gettemplateArraysClassUUIDs(unittest.TestCase):
    def setUp(self):
        self.variable = {
            'ZipFile': "templates_wen_v12.zip",
            'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
            'TableName_IMU': 'dailyimu',
            'DataBase': 'dailyimureportdb',
            'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
        }
    # test the number of templatearray while getting template file from s3
    def test_the_number_of_templatearray(self):
        Variables = self.variable
        clean_tmp_file()
        AllTemplatesArrays = loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0]
        result = len(gettemplateArraysClassUUIDs(AllTemplatesArrays)[0])
        print(gettemplateArraysClassUUIDs(AllTemplatesArrays)[2])
        expected= 240
        self.assertEqual(result, expected)
    # test if the classification of first templatearray is harshbraking
    def test_single_category_of_first_template(self):
        Variables = self.variable
        clean_tmp_file()
        AllTemplatesArrays = loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0]
        result = gettemplateArraysClassUUIDs(AllTemplatesArrays)[1][0]
        expected = 'harshBraking'
        self.assertEqual(result, expected)

#add_new_template_to_zip_file
class Test_add_new_template_to_zip_file(unittest.TestCase):
    def setUp(self):
        self.variable = {
            'ZipFile': "templates_wen_v12.zip",
            'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
            'TableName_IMU': 'dailyimu',
            'DataBase': 'dailyimureportdb',
            'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
        }
    # test the number of templates after adding new template
    def test_the_number_of_templatearray(self):
        Variables = self.variable
        clean_tmp_file()
        test_template_data_query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                                   "from " + Variables['TableName_IMU'] + " " \
                                   "where  predictedlabel = 'harshBraking' "
        template_rawdata = get_template_rawdata_from_athena(test_template_data_query)
        result = len(add_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile'],template_rawdata))
        expected = 241
        self.assertEqual(result, expected)

    # test if the classification of last templatearray is harshbraking
    def test_the_single_category_of_latest_template(self):
        Variables = self.variable
        clean_tmp_file()
        test_template_data_query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                                   "from " + Variables['TableName_IMU'] + " " \
                                   "where  predictedlabel = 'harshBraking' "
        template_rawdata = get_template_rawdata_from_athena(test_template_data_query)
        #print(type(add_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile'],template_rawdata)[-1]))
        result = add_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile'],template_rawdata)[-1]['Instances']['prediction']['predicted_label']
        expected = 'harshBraking'
        self.assertEqual(result, expected)


class Test_delete_new_template_to_zip_file(unittest.TestCase):
    def setUp(self):
        self.variable = {
            'ZipFile': "templates_wen_v12.zip",
            'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
            'TableName_IMU': 'dailyimu',
            'DataBase': 'dailyimureportdb',
            'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
        }
    # test the number of templates after deleting new template
    def test_the_number_of_templatearray(self):
        Variables = self.variable
        clean_tmp_file()

        result = len(delete_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile']))
        expected = 239
        self.assertEqual(result, expected)

    # test if the classification of last templatearray is bump
    def test_the_single_category_of_last_template(self):
        Variables = self.variable
        # print(type(add_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile'],template_rawdata)[-1]))
        result = delete_new_template_to_zip_file(Variables['S3BucketName'], Variables['ZipFile'])[-1][
            'Instances']['prediction']['predicted_label']
        expected = 'bump'
        self.assertEqual(result, expected)

class Test_get_template_rawdata_from_athena(unittest.TestCase):
    def setUp(self):
        self.query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                     "from dailyimu " \
                     "where  predictedlabel = 'harshBraking' " \
                     "limit 1 "
    #test if the rawdata from athena query is dict
    def test_result_format(self):
        test_template_data_query = self.query

        result = type(get_template_rawdata_from_athena(test_template_data_query))
        expected = dict
        self.assertEqual(expected, result)

    # test if the classification of the rawdata from athena query is harshbraking
    def test_the_single_category_of_last_template(self):
        test_template_data_query = self.query

        result = get_template_rawdata_from_athena(test_template_data_query)[
            'Instances']['prediction']['predicted_label']
        expected = 'harshBraking'
        self.assertEqual(expected, result)
#upload_new_templates_file_to_S3

class Test_upload_new_templates_file_to_S3(unittest.TestCase):
    def setUp(self):
        self.variable = {
            'ZipFile': "templates_wen_v12.zip",
            'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
            'TableName_IMU': 'dailyimu',
            'DataBase': 'dailyimureportdb',
            'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
        }
    def test_the_number_of_latest_zip_file(self):
        Variables = self.variable

        test_template_data_query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                                   "from " + Variables['TableName_IMU'] + " " \
                                   "where  predictedlabel = 'harshBraking' "
        test_template_rawdata = get_template_rawdata_from_athena(test_template_data_query)
        upload_new_templates_file_to_S3(Variables['S3BucketName'], Variables['ZipFile'], test_template_rawdata)

        result = len(loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0])
        expected = 241
        self.assertEqual(expected, result)

class Test_delete_new_templates_file_to_S3(unittest.TestCase):
    def setUp(self):
        self.variable = {
            'ZipFile': "templates_wen_v12.zip",
            'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod",
            'TableName_IMU': 'dailyimu',
            'DataBase': 'dailyimureportdb',
            'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
        }
    def test_the_number_of_latest_zip_file(self):
        Variables = self.variable
        delete_new_templates_file_to_S3(Variables['S3BucketName'], Variables['ZipFile'],)

        result = len(loadTemplatesFromS3(Variables['S3BucketName'], Variables['ZipFile'])[0])
        expected = 240
        self.assertEqual(expected, result)