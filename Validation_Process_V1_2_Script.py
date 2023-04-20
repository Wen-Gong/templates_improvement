from Functions.validation import *
from Functions.Update_template import *

Variables = {
        'DataBase': 'dailyimureportdb',
        'TableName_IMU': "dailyimu",
        'S3BucketNameSaveQuery': 'aws-athena-query-results-us-east-1-444235434904',
        'S3BucketName': "444235434904-pvcam-s3-blackbox-template-store-prod"
    }
def handler():
    # get daily imu data with matching score already from s3
    # temporary get it from local environment
    data_from_json: TextIO = open("./temporary file/daily_imu_review_data_with_score.json")
    events_dict: list = json.load(data_from_json)

    # get latest version and plus 1 to this number as newest version
    version = get_latest_version_of_template_file(Variables['S3BucketName'])

    # Validation Process 1

    # extract misclassified data as potential template data from daily imu data and the rest data as validation data
    # try to filter out some not sure classification data

    valid_correct_class = [item for category_list in list(Event_category_from_daily_imu_data.values()) for item in category_list]
    valid_events = [event for event in events_dict if event['Correct Classification'] in valid_correct_class]
    valid_events = [event for event in valid_events if event['Event Type Correct?'] not in ['Data Issue']]
    misclassfied_events = [event for event in valid_events if event['Event Type Correct?']=='No']

    #run the Validation Process 1
    for potential_event in misclassfied_events:
        #divide validation data and get some key parameter
            validation_events = [event for event in valid_events if event!=potential_event]
            comfirmed_class_data, other_class_data, new_comfirmed_class = divide_validation_events_by_classification(validation_events,potential_event['Correct Classification'],Event_category_from_daily_imu_data)
            potential_template_key = new_comfirmed_class+'_'+json.loads(potential_event['rawdata'])['MetaData']['id']

            #get 4 key validation value
            overall_mean_value_of_comfirmed_class_data, final_top_3_result_impact_rate_to_same_classification_events = get_overall_mean_value_and_top_3_result(comfirmed_class_data,potential_event,new_comfirmed_class,potential_template_key)
            overall_mean_value_of_other_class_data, final_top_3_result_impact_rate_to_other_classification_events = get_overall_mean_value_and_top_3_result(other_class_data,potential_event,new_comfirmed_class,potential_template_key)
            add_template_performance_of_v1(overall_mean_value_of_comfirmed_class_data,
                                           final_top_3_result_impact_rate_to_same_classification_events,
                                           overall_mean_value_of_other_class_data,
                                           final_top_3_result_impact_rate_to_other_classification_events,
                                           potential_event,
                                           new_comfirmed_class,
                                           version)

            valid_events = [potential_event] + comfirmed_class_data + other_class_data

    # Validation Process 2

    #run the Validation Process 2

    #get good templates group and new validation events for v2 process
    potential_templates_group = []
    for event in valid_events:
        if 'Template_performance_v1' in event.keys() and \
                event['Template_performance_v1'][version]['template_final_result']['template_evaluation_v1'] in ['good template', 'Wonderful Template']:
            potential_templates_group.append(event)
    potential_templates_group_keys = [every_template['new_predicted_label'] + '_' + json.loads(every_template['rawdata'])['MetaData']['id'] for
        every_template in potential_templates_group]
    validation_events_v2 = [event for event in valid_events if event not in potential_templates_group]

    # get new top 3 for validation events
    for event in validation_events_v2:
        new_top3_and_final_result_of_every_event(event, version, potential_templates_group_keys)

    #get 3 key validation rate to potential template
    for i in range(0, len(potential_templates_group)):
        get_key_validation_rate_v2(potential_templates_group_keys[i], Event_category_from_daily_imu_data,
                                   validation_events_v2, potential_templates_group[i],version)

    # judge confirmed template or failed template
    for template in potential_templates_group:
        judge_of_v2(template, version)

    confirmed_templates = [template for template in potential_templates_group if template['Template_performance_v2'][version][
                               'template_evaluation_v2'] == 'confirmed template']

    print(confirmed_templates)
    #temporary save it locally
    with open("./temporary file/confirmed_templates.json", "w") as outfile:
        json.dump(confirmed_templates, outfile)

handler()