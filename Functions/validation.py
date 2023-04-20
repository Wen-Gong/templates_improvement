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
from Functions.Update_template import gettemplateArraysClassUUIDs
from collections import Counter
import distance


Event_category_from_daily_imu_data={'harshBraking': ['Braking','Harsh Braking'],
                                    'harshLeftTurn':['Left','Harsh Left Turn'],
                                    'harshRightTurn': ['Right','Harsh Right Turn'],
                                    'bump':['Bump','Non Event','Slow Speed Event'],
                                    'harshAcceleration':['Acceleration','Harsh Acceleration']}
#get latest version of zip file
def get_latest_version_of_template_file(bucket_path: str):

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_path)
    file_name_of_bucket = [key.key for key in bucket.objects.all() if key.size]
    max_version = max([i for file_name in file_name_of_bucket if file_name[:11] == 'templates_v' for i in file_name if i.isdigit()])
    old_version = 'v'+max_version
    new_version ='v' + str(int(max_version)+1)

    old_version_file = "templates_"+ old_version +".zip"
    new_version_file = "templates_" + new_version + ".zip"

    return new_version,old_version,new_version_file,old_version_file

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

############
# get matching score dict based on one event with all templates

def matching_score_dict_with_current_templates_file(templates_data:list[[list]],xyz_points_array:np.array) -> dict:

    templatesarrays,Class,ids = gettemplateArraysClassUUIDs(templates_data)
    matching_score_dict = {}
    for i in range(0,len(templatesarrays)):
        score = match_score_with_one_template(templatesarrays[i],xyz_points_array)
        matching_score_dict[Class[i]+'_'+ids[i]] = score

    return matching_score_dict

def add_matching_score_dict_to_datadict(matching_score_dict:dict,datadict:dict):
    datadict['matching_score_list'] = matching_score_dict

# add matching score dict to the main daily_imu_review data
def data_with_matching_score_list(daily_imu_review_dict:dict,templates_data:list) -> dict:

    for piece_data in daily_imu_review_dict:
        if piece_data['rawdata']!=None:
            xyz_points_array = extract_accelemeter_data_from_list_to_array(json.loads(piece_data['rawdata'])['Instances']['features']['featurevector'])
            matching_score_dict = matching_score_dict_with_current_templates_file(templates_data,xyz_points_array)
            piece_data['matching_score_list'] = matching_score_dict
        else:
            piece_data['matching_score_list'] = None

    return daily_imu_review_dict

###########

#get key from values
def comfirm_key(dict, value):
   return [k for (k,v) in dict.items() if value in v][0]


# divide validation data according to daily imu data
def divide_validation_events_by_classification(validation_events:list[dict],correct_classification_name:str,Event_category: dict) -> list[list,list,str]:
    if correct_classification_name !=None:
        valid_correct_class = [item for category_list in list(Event_category.values()) for item in category_list]
        if correct_classification_name in valid_correct_class:

            correct_classification_name = correct_classification_name
        else:
            similar_words_list = [(word, distance.levenshtein(correct_classification_name, word)) for word in valid_correct_class]
            similar_words_list.sort(key=lambda x: x[1])
            correct_classification_name = similar_words_list[0][0]

        comfirmed_class = comfirm_key(Event_category, correct_classification_name)
        comfirmed_classification_data = [event for event in validation_events if event['Correct Classification'] in Event_category[comfirmed_class]]
        other_classification_data = [event for event in validation_events if event['Correct Classification'] not in Event_category[comfirmed_class] and event['Correct Classification'] in valid_correct_class]

        return comfirmed_classification_data, other_classification_data, comfirmed_class

def confirm_value(dict, key_word):

   return [v for (k,v) in dict.items() if key_word in k],[k for (k,v) in dict.items() if key_word in k]

def score_of_one_template_with_one_event(template_data:dict,one_event_data:dict):

    if one_event_data['rawdata']!=None and template_data['rawdata']!= None:
        xyz_points_array_event = extract_accelemeter_data_from_list_to_array(json.loads(one_event_data['rawdata'])['Instances']['features']['featurevector'])
        xyz_points_array_template = extract_accelemeter_data_from_list_to_array(json.loads(template_data['rawdata'])['Instances']['features']['featurevector'])
        matching_score = match_score_with_one_template(xyz_points_array_event,xyz_points_array_template)

        return matching_score
    else:
        return None

#create 'matching_score_list_with_potential_template' in every validation event
def create_matching_score_list_with_potential_templates(event_data:dict,template_data:dict,potential_template_key):

    if 'matching_score_list_with_potential_template' not in event_data.keys():
        event_data['matching_score_list_with_potential_template'] = {
            potential_template_key:score_of_one_template_with_one_event(event_data,template_data)
        }

    else:
        event_data['matching_score_list_with_potential_template'][potential_template_key] = score_of_one_template_with_one_event(event_data,template_data)

def mean_difference_value_every_event(event: dict,potential_event:dict,new_comfirmed_class:str,potential_template_key:str):
    # before mean value
    before_score_list,template_classes_and_id = confirm_value(event['matching_score_list'], new_comfirmed_class)
    before_mean_score_each_event = np.mean(before_score_list)

    #after mean value
    #get potential rawdata and get matching score with one event, save to matching_score_list, then get after mean score
    create_matching_score_list_with_potential_templates(event,potential_event,potential_template_key)

    score_list_of_p_temp_and_event,potential_template_classes_and_id = confirm_value(event['matching_score_list_with_potential_template'], potential_template_key)

    if score_list_of_p_temp_and_event[0]!=None:

        after_score_list,template_classes_and_id_after = [before_score_list+score_list_of_p_temp_and_event,template_classes_and_id+potential_template_classes_and_id]#[score,template_name]
        after_mean_score_each_event = np.mean(after_score_list)

    else:
        after_score_list, template_classes_and_id_after = [before_score_list,
                                                           template_classes_and_id]  # [score,template_name]
        after_mean_score_each_event = np.mean(after_score_list)

    mean_difference = before_mean_score_each_event - after_mean_score_each_event

    return mean_difference

def top3_result_of_one_event(potential_template_key:str,event_data:dict):

    original_templates_prediction = list(event_data['matching_score_list'].items())
    potential_template_prediction = [tuple([potential_template_key,event_data['matching_score_list_with_potential_template'][potential_template_key]])]
    all_templates_prediction = original_templates_prediction + potential_template_prediction
    all_templates_prediction.sort(key=lambda a: a[1])
    top3_result_of_one_event = ['existed_in_top_3' for (k,v) in all_templates_prediction[:3] if k == potential_template_key]

    return top3_result_of_one_event

def get_overall_mean_value_and_top_3_result(validation_dataset:list[dict],potential_event,new_comfirmed_class,potential_template_key):
    overall_mean_value_list = []
    top_3_result_impact_list = []

    for each_event in validation_dataset:
        mean_difference_value_from_every_event = mean_difference_value_every_event(each_event,potential_event,new_comfirmed_class,potential_template_key)
        overall_mean_value_list.append(mean_difference_value_from_every_event)
        top_3_result_impact_list_of_every_event = top3_result_of_one_event(potential_template_key,each_event)
        top_3_result_impact_list = top_3_result_impact_list + top_3_result_impact_list_of_every_event

    overall_mean_value = np.mean(overall_mean_value_list)
    top_3_result_impact_rate = len(top_3_result_impact_list) / len(validation_dataset)

    return overall_mean_value,top_3_result_impact_rate

def add_template_performance_of_v1(overall_mean_value_1:float,final_top_3_result_impact_rate_1:float,overall_mean_value_2:float,final_top_3_result_impact_rate_2:float,potential_event:dict,new_comfirmed_class:str,version):

    template_performance_details = {'over_mean_score_is_positive_result': overall_mean_value_1,
                                  'over_mean_score_is_negative_result': overall_mean_value_2,
                                  'effect_the_same_classification': final_top_3_result_impact_rate_1,
                                  'Does_not_effect_the_other_classification_result': final_top_3_result_impact_rate_2,}
    Result= {}
    if overall_mean_value_1 >=0:
        Result['over_mean_score_is_positive']=True
    else:
        Result['over_mean_score_is_positive']=False

    if overall_mean_value_2 <=0:
        Result['over_mean_score_is_negative']=True
    else:
        Result['over_mean_score_is_negative']=False

    if final_top_3_result_impact_rate_1>=0:
        Result['effect_the_same_classification'] = True
    else:
        Result['effect_the_same_classification'] = False

    if final_top_3_result_impact_rate_2<=0.1:
        Result['Does_not_effect_the_other_classification'] = True
    else:
        Result['Does_not_effect_the_other_classification'] = False

    template_final_result={}
    template_final_result['template_evaluation_v1'] = Decide_good_template(Result)

    update_dict = {version:{
                'template_performance_details':template_performance_details,
                'template_performance_result':Result,
                'template_final_result': template_final_result
            }}

    if 'Template_performance_v1' in potential_event.keys():
        potential_event['Template_performance_v1'].update(update_dict)
    else:
        potential_event['Template_performance_v1']= update_dict
    potential_event['new_predicted_label'] = new_comfirmed_class

    return 'add template result successfully'

########################
#validation process 2
######################

def new_top3_and_final_result_of_every_event(validation_event:list,version:str,potential_templates_group_keys:str):

    new_templates_group_prediction=[(k,v) for (k,v) in validation_event['matching_score_list_with_potential_template'].items() if k in potential_templates_group_keys]+list(validation_event['matching_score_list'].items())# check version
    new_templates_group_prediction.sort(key=lambda a: a[1])
    top3_label = [template_key.split('_')[0] for template_key in list(dict(new_templates_group_prediction[:3]).keys())]
    new_final_result = Counter(top3_label).most_common(1)[0][0]
    update_dict = {version: {'new_top_3': dict(new_templates_group_prediction[:3]),
                             'new_final_result': new_final_result}}
    if 'new_predict_result_before_rules_valid_2' in validation_event.keys():
        validation_event['new_predict_result_before_rules_valid_2'].update(update_dict)
    else:
        validation_event['new_predict_result_before_rules_valid_2'] = update_dict


def influenced_value_of_validation_2(potential_template_key:str,Event_category_from_daily_imu_data:dict, validation_events:list,top_number:int,version:str):

    influenced_events_number = sum([1 for event in validation_events if potential_template_key in list(event['new_predict_result_before_rules_valid_2'][version]['new_top_3'].keys())[top_number]])

    influenced_correct_events_number = sum([1 for event in validation_events if potential_template_key in list(event['new_predict_result_before_rules_valid_2'][version]['new_top_3'].keys())[top_number] and potential_template_key.split('_')[0] == comfirm_key(Event_category_from_daily_imu_data,event['Correct Classification'])])

    influenced_wrong_events_finally_number = sum([1 for event in validation_events if potential_template_key in list(event['new_predict_result_before_rules_valid_2'][version]['new_top_3'].keys())[top_number] and potential_template_key.split('_')[0] == event['new_predict_result_before_rules_valid_2'][version]['new_final_result'] and event['new_predict_result_before_rules_valid_2'][version]['new_final_result'] !=comfirm_key(Event_category_from_daily_imu_data,event['Correct Classification'])])

    return influenced_events_number,influenced_correct_events_number, influenced_wrong_events_finally_number

def get_key_validation_rate_v2(potential_template_key:str,Event_category_from_daily_imu_data:dict, validation_events,template_event_data:dict,version:str):

    influenced_events_number_1,influenced_correct_events_number_1, influenced_wrong_events_finally_number_1 = influenced_value_of_validation_2(potential_template_key,
                                                                                                                                               Event_category_from_daily_imu_data,
                                                                                                                                               validation_events,0,version)
    influenced_events_number_2,influenced_correct_events_number_2, influenced_wrong_events_finally_number_2 = influenced_value_of_validation_2(potential_template_key,
                                                                                                                                               Event_category_from_daily_imu_data,
                                                                                                                                               validation_events,1,version)
    influenced_events_number_3,influenced_correct_events_number_3, influenced_wrong_events_finally_number_3 = influenced_value_of_validation_2(potential_template_key,
                                                                                                                                               Event_category_from_daily_imu_data,
                                                                                                                                               validation_events,2,version)

    total_influenced_event_number = influenced_events_number_1+influenced_events_number_2+influenced_events_number_3
    total_influenced_correct_events_number = influenced_correct_events_number_1+influenced_correct_events_number_2+influenced_correct_events_number_3
    total_influenced_wrong_events_finally_number = influenced_wrong_events_finally_number_1+influenced_wrong_events_finally_number_2+influenced_wrong_events_finally_number_3

    try:
        correct_detected_rate_top3 = total_influenced_correct_events_number/total_influenced_event_number
        wrong_detected_rate_final = total_influenced_wrong_events_finally_number/total_influenced_event_number
    except:
        correct_detected_rate_top3 = 0
        wrong_detected_rate_final = 0
    wrong_detected_rate_top3 = 1 - correct_detected_rate_top3

    update_dict = {version:{'template_performance_details':{'correct_detected_rate_top3':correct_detected_rate_top3,
                                                            'correct_detected_rate_top3':wrong_detected_rate_top3,
                                                             'wrong_detected_rate_final':wrong_detected_rate_final,
                                                            'influenced_events_numbers':(influenced_events_number_1,influenced_events_number_2,influenced_events_number_3),
                                                            'influenced_correct_events_numbers':(influenced_correct_events_number_1,influenced_correct_events_number_2,influenced_correct_events_number_3),
                                                             'influenced_wrong_events_finally_numbers':(influenced_wrong_events_finally_number_1,
                                                                                                           influenced_wrong_events_finally_number_2,
                                                                                                           influenced_wrong_events_finally_number_3)}}}
    if 'Template_performance_v2' in template_event_data.keys():
        template_event_data['Template_performance_v2'].update(update_dict)
    else:
        template_event_data['Template_performance_v2']=update_dict

def judge_of_v2(template_event_data:dict,version:str):
    if version in template_event_data['Template_performance_v2'].keys():
        if template_event_data['Template_performance_v2'][version]['template_performance_details']['correct_detected_rate_top3'] >=0.85 and template_event_data['Template_performance_v2'][version]['template_performance_details']['wrong_detected_rate_final']<0.05:
            template_event_data['Template_performance_v2'][version].update({'template_evaluation_v2':'confirmed template'})
        else:
            template_event_data['Template_performance_v2'][version].update({'template_evaluation_v2':'failed template'})

###############
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
    #print(len(templates_value_data))
    for i in range(0, len(templates_value_data)):
        if templates_value_data.columns[-1] in templates_value_data.iloc[i].sort_values(ascending=True).head(3).index:
            n = n + 1
    rate = n/len(templates_value_data)

    return rate

# Get the result of conditional judgement
def final_result_of_conditional_judgement(rawdata: dict,template_array:np.array,validationdata_internal: pd.DataFrame, validationdata_external:pd.DataFrame,original_label:str,category_name: str)-> dict:

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
        Result['effect_the_same_classification'] = True
    else:
        Result['effect_the_same_classification'] = False

    if top_three_rate(validationdata_external)<=0.1:
        Result['Does_not_effect_the_other_classification'] = True
    else:
        Result['Does_not_effect_the_other_classification'] = False

    result_details = {'over_mean_score_is_positive_result': template_performance(template_array,validationdata_internal,category_name)[1]['mean'],
                      'over_mean_score_is_negative_result': template_performance(template_array,validationdata_external,category_name)[1]['mean'],
                      'effect_the_same_classification': top_three_rate(validationdata_internal),
                      'Does_not_effect_the_other_classification_result': top_three_rate(validationdata_external),
                      }


    Final_result = {'original_label':original_label,'category':category_name,'template_quality': Decide_good_template(Result),'result':Result,'result_details':result_details,'Rawdata':rawdata}

    return Final_result

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
        other_classification_data = validation_data[~validation_data['Noevent category'].isin(['braking','normal braking'])]

    elif category_name =='harshLeftTurn':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['normal left turning','left turning'])]
        other_classification_data = validation_data[~validation_data['Noevent category'].isin(['normal left turning','left turning'])]

    if category_name =='harshRightTurn':
        real_original_data   = validation_data[validation_data['Noevent category'].isin(['normal right turning','right turning'])]
        other_classification_data = validation_data[~validation_data['Noevent category'].isin(['normal right turning','right turning'])]

    if category_name =='bump':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['bump','braking/bump','small bump'])]
        other_classification_data = validation_data[~validation_data['Noevent category'].isin(['bump','braking/bump','small bump'])]

    if category_name =='harshAcceleration':
        real_original_data  = validation_data[validation_data['Noevent category'].isin(['accelerating','normal accelerating'])]
        other_classification_data = validation_data[~validation_data['Noevent category'].isin(['accelerating','normal accelerating'])]

    return real_original_data, other_classification_data



#potential_template_data_query = "select deviceid, rawdata, predictedlabel, eventtime, vehiclename, orgname " \
                                 #"from " + Variables['TableName_IMU'] + " " \
                                # "where  predictedlabel = 'harshBraking' " \
                                # "limit 1 "

#template_array=get_template_array_from_athena(potential_template_data_query)
#validation_data=pd.read_csv('C:/Users/WenGong/Downloads/templates_improvement_staging/validation_data.csv')
#print(template_performance(template_array,validation_data[validation_data['Noevent_category']=='braking'],'harshBraking')[0])
#print(validation_data[validation_data['Noevent category'].isin(['normal left turning','left turning'])])

