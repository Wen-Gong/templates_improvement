import pandas as pd
import zipfile
import glob
import boto3
import numpy as np
import json
import fastdtw
import os
import time
import io

Variables = {
        'DataBase': 'dailyimureportdb',
        'TableName_IMU': "dailyimu",
        'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904'
    }

def execute_query(query):
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

#extract xyz datapoints
def extract_accelemeter_data_from_str_to_array(string_datapoints:str) -> list[list]:

    list_datapoints=eval(string_datapoints)
    xyz_points = [[float(sublist[1]),float(sublist[2]),float(sublist[3])] for sublist in list_datapoints]

    return xyz_points

def extract_accelemeter_data_from_list_to_array(list_datapoints:list[[list]]) -> np.array([np.array(list)]):

    xyz_points = np.array([np.array([float(sublist[1]),float(sublist[2]),float(sublist[3])]) for sublist in list_datapoints])

    return xyz_points

#get matching score of potential template and one piece of validate data
def match_score_with_one_template(template,xyz_points_array) -> float:

    score = fastdtw.fastdtw(template, xyz_points_array)[0]

    return score

def score_list_with_all_templates(alltemplateArrays,xyz_points_array) -> list:

    Score_list=[]
    for template in alltemplateArrays:
                    score = match_score_with_one_template(template,xyz_points_array)
                    Score_list.append(score)

    return Score_list

#get matching score list of potential template and all of validate data
def score_list_with_validation(validation_dataset,xyz_points_array) -> list:

    Score_list=[]
    for validation_data in validation_dataset:
                    score = match_score_with_one_template(validation_data,xyz_points_array)
                    Score_list.append(score)

    return Score_list

#get mean value list for one category
def mean_list_of_single_category(validation_dataset:pd.DataFrame,category_name:str)-> list:

    template_names=[column_name for column_name in validation_dataset.columns if 'template' in column_name]
    category_names=[column_name for column_name in template_names if category_name in column_name.split('_')[0]]
    mean_value_list=[]
    for i in validation_dataset.index:
        mean_value=np.mean([validation_dataset[category_name][i] for category_name in category_names])
        mean_value_list.append(mean_value)

    return mean_value_list

# once adding new template, get new dataframe including the new template and the statistical values of differece between before and after
def template_performance(template_array:np.array,validation_dataset:pd.DataFrame,category_label:str):
    allvalidationArrays=[]
    for piece_data in validation_dataset['rawdata']:
        array_data=extract_accelemeter_data_from_list_to_array(json.loads(piece_data)['Instances']['features']['featurevector'])
        allvalidationArrays.append(array_data)
    score_list_of_new_template=score_list_with_validation(allvalidationArrays,template_array)
    validation_data_new=validation_dataset.copy()
    columnname=category_label+'_template_'+str(int(validation_dataset.columns[-1].split('_')[-1])+1)
    validation_data_new[columnname]=score_list_of_new_template

    mean_list_before=mean_list_of_single_category(validation_dataset,category_label)
    mean_list_after=mean_list_of_single_category(validation_data_new,category_label)

    stats_of_difference_values=pd.Series(np.array(mean_list_before)- np.array(mean_list_after)).describe()

    return validation_data_new,stats_of_difference_values


def get_template_array_from_athena(athena_query:str)-> np.array:
    s3 = boto3.resource('s3')
    potential_template_data_query_request_id = execute_query(athena_query)
    time.sleep(5)
    potential_template_data_csv = s3.Bucket(Variables['S3BucketNameSaveQuery']).Object(
        key=potential_template_data_query_request_id + '.csv').get()
    potential_template_rawdata = pd.read_csv(io.BytesIO(potential_template_data_csv['Body'].read()), encoding='utf8')[
        'rawdata']

    template_array = extract_accelemeter_data_from_list_to_array(
        json.loads(potential_template_rawdata[0])['Instances']['features']['featurevector'])

    return template_array


# get the rate shows that how many cases in which the new template becomes to one of top three templates
def top_three_rate(validationdata:pd.DataFrame)-> float:
    templates_value_data = validationdata.loc[:, validationdata.columns.str.contains('template')]
    n = 0
    print(len(templates_value_data))
    for i in range(0, len(templates_value_data)):
        if templates_value_data.columns[-1] in templates_value_data.iloc[i].nsmallest(3).index:
            n = n + 1
    rate = n/len(templates_value_data)

    return rate

# Get the result of conditional judgement
def final_result_of_conditional_judgement(template_array:np.array,validationdata_internal: pd.DataFrame, validationdata_external:pd.DataFrame,category_name: str)-> dict:

    Result= {}
    if template_performance(template_array,validationdata_internal,category_name)[1]['mean'] >=0:
        Result['over_mean_score_is_positive']=True
    else:
        Result['over_mean_score_is_positive']=False

    if template_performance(template_array,validationdata_external,category_name)[1]['mean'] <=0:
        Result['over_mean_score_is_negative']=True
    else:
        Result['over_mean_score_is_negative']=False

    if top_three_rate(validationdata_internal)>=0:
        Result['effect the same classification'] = True
    else:
        Result['effect the same classification'] = False

    if top_three_rate(validationdata_external)<=0.03:
        Result['Does not effect the other classification'] = True
    else:
        Result['Does not effect the other classification'] = False


    return Result

# Judge if the template is wonderful, good or bad
def Decide_good_template(Result: dict)-> str:

    if list(Result.values()).count(True) ==4:
        return 'Wonderful Template'
    elif list(Result.values()).count(True) ==3:
        return 'good template'
    elif list(Result.values()).count(True) <=2:
        return 'bad template'

# divide validation data according to current format of validation data
def divide_dataset_by_classification(validation_data,category_name)-> [pd.DataFrame,pd.DataFrame]:
    if category_name =='harshBraking':
        real_original_data = validation_data[validation_data['Noevent category'].isin(['braking','normal braking'])]
        other_classification_data = validation_data[validation_data['Noevent category'].isin(['braking','normal braking'])]

    elif category_name =='harshLeftTurn':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['normal left turning','left turning'])]
        other_classification_data = validation_data[validation_data['Noevent category'].isin(['normal left turning','left turning'])]

    if category_name =='harshRightTurn':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['normal right turning','right turning'])]
        other_classification_data = validation_data[validation_data['Noevent category'].isin(['normal right turning','right turning'])]

    if category_name =='bump':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['bump','braking/bump'])]
        other_classification_data = validation_data[validation_data['Noevent category'].isin(['bump','braking/bump'])]

    if category_name =='harshAcceleration':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['accelerating','normal accelerating'])]
        other_classification_data = validation_data[validation_data['Noevent category'].isin(['accelerating','normal accelerating'])]

    return real_original_data, other_classification_data



#potential_template_data_query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                                 #"from " + Variables['TableName_IMU'] + " " \
                                # "where  predictedlabel = 'harshBraking' " \
                                # "limit 1 "

#template_array=get_template_array_from_athena(potential_template_data_query)
#validation_data=pd.read_csv('C:/Users/WenGong/Downloads/templates_improvement_staging/validation_data.csv')
#print(template_performance(template_array,validation_data[validation_data['Noevent_category']=='braking'],'harshBraking')[0])
#print(validation_data[validation_data['Noevent category'].isin(['normal left turning','left turning'])])

