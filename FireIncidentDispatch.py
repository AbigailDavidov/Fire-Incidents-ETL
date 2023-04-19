import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine as ce
from dotenv import load_dotenv
import os

class FireIncidenceETL():
    def __init__(self):
        load_dotenv()
        # using sqlite db
        self.db_file = ce('sqlite:///C:/Users/Home/Downloads/FireIncidents/FireIncidentsDB.db')

        # connecting to API and pullling data
        self.url = "https://data.cityofnewyork.us/resource/8m42-w767"
        self.username = os.environ["USERNAME_"]
        self.password = os.environ["PASSWORD_"]
        self.response = requests.get(self.url, auth=(self.username, self.password))

        self.alarm_boxes_df = self.list_to_df(self.create_alarm_boxes_list())
        self.locations_df = self.list_to_df(self.create_locations_list())
        self.classification_groups_df = self.list_to_df(self.create_classification_G_list())
        self.classifications_df = self.list_to_df(self.create_classification_list())
        self.classifications_df = self.addforeignKey(self.classifications_df,self.classification_groups_df)
        self.incidents_df = self.create_incidents_df()
        self.upload_to_db()

    # save record's alarm box info
    def create_alarm_boxes_list(self):
        alarm_boxes = []
        for i in self.response.json():
            alarm_boxes_dict = {'alarm_box_borough': i['alarm_box_borough'], 'alarm_box_number': i['alarm_box_number'],
                            'alarm_box_location': i['alarm_box_location'],
                            'alarm_source_description_tx': i['alarm_source_description_tx'],
                            'alarm_level_index_description': i['alarm_level_index_description'],
                            'highest_alarm_level': i['highest_alarm_level']}
            alarm_boxes.append(alarm_boxes_dict)
        return alarm_boxes

    # save record's district info
    def create_locations_list(self):
        locations = []
        for i in self.response.json():
            locations_dict = {}
            locations_dict["zipcode"] = self.existsInDict(i, "zipcode")
            locations_dict["policeprecinct"] = self.existsInDict(i, "policeprecinct")
            locations_dict["citycouncildistrict"] = self.existsInDict(i, "citycouncildistrict")
            locations_dict["communitydistrict"] = self.existsInDict(i, "communitydistrict")
            locations_dict["communityschooldistrict"] = self.existsInDict(i, "communityschooldistrict")
            locations_dict["congressionaldistrict"] = self.existsInDict(i, "congressionaldistrict")
            locations_dict["incident_borough"] = self.existsInDict(i, "incident_borough")
            locations.append(locations_dict)
        return locations

    # save record's classification info
    def create_classification_list(self):
        classifications = []
        for i in self.response.json():
            classifications_dict = {'incident_classification': i['incident_classification'], 'incident_classification_group': i['incident_classification_group']}
            classifications.append(classifications_dict)
        return classifications

    # save record's classification groups info
    def create_classification_G_list(self):
        classification_groups = []
        for i in self.response.json():
            classification_groups_dict = {'incident_classification_group': i['incident_classification_group']}
            classification_groups.append(classification_groups_dict)
        return classification_groups

    # taking care of foreign key in classifications table
    def addforeignKey(self, classifications_df, classification_groups_df):
        for i in classifications_df.to_dict('records'):
            for group in classification_groups_df.to_dict('records'):
                if i['incident_classification_group'] == group['incident_classification_group']:
                    i['incident_classification_group'] = group['ID']
        return classifications_df

    def existsInDict(self, dict, key):
        if key in dict:
            return dict[key]
        else:
            return " "

    # creating incidents df considering foreign keys
    def create_incidents_df(self):
        incidents = []
        for i in self.response.json():
            incidents_dict = {'starfire_incident_id': i['starfire_incident_id'],
                              'incident_datetime': i['incident_datetime'],
                              'district_id': i['incident_borough'], 'alarm_box_id': i['alarm_box_number'],
                              'incident_classification': i['incident_classification'],
                              'dispatch_response_seconds_qy': i['dispatch_response_seconds_qy'],
                              'first_assignment_datetime': i['first_assignment_datetime'],
                              'incident_close_datetime': i['incident_close_datetime'],
                              'valid_dispatch_rspns_time_indc': i['valid_dispatch_rspns_time_indc'],
                              'valid_incident_rspns_time_indc': i['valid_incident_rspns_time_indc'],
                              'incident_response_seconds_qy': i['incident_response_seconds_qy'],
                              'incident_travel_tm_seconds_qy': i['incident_travel_tm_seconds_qy'],
                              'engines_assigned_quantity': i['engines_assigned_quantity'],
                              'ladders_assigned_quantity': i['ladders_assigned_quantity'],
                              'other_units_assigned_quantity': i['other_units_assigned_quantity'],
                              'alarm_box_borough': i['alarm_box_borough'], 'alarm_box_number': i['alarm_box_number'],
                              'alarm_box_location': i['alarm_box_location'],
                              'alarm_source_description_tx': i['alarm_source_description_tx'],
                              'alarm_level_index_description': i['alarm_level_index_description'],
                              'highest_alarm_level': i['highest_alarm_level'], "zipcode": self.existsInDict(i, "zipcode"),
                              "policeprecinct": self.existsInDict(i, "policeprecinct"),
                              "citycouncildistrict": self.existsInDict(i, "citycouncildistrict"),
                              "communitydistrict": self.existsInDict(i, "communitydistrict"),
                              "communityschooldistrict": self.existsInDict(i, "communityschooldistrict"),
                              "congressionaldistrict": self.existsInDict(i, "congressionaldistrict"),
                              "incident_borough": self.existsInDict(i, "incident_borough"),
                              "first_activation_datetime": self.existsInDict(i, "first_activation_datetime"),
                              "first_on_scene_datetime": self.existsInDict(i, "first_on_scene_datetime")}

            # saving into future tables
            incidents.append(incidents_dict)
        for i in incidents:
            for j in self.classifications_df.to_dict('records'):
                if i['incident_classification'] == j['incident_classification']:
                    i['incident_classification'] = j['ID']
            for j in self.locations_df.to_dict('records'):
                if j["zipcode"] == i["zipcode"] and j["policeprecinct"] == i["policeprecinct"] and j[
                    "citycouncildistrict"] == i["citycouncildistrict"] and j["communitydistrict"] == i["communitydistrict"] \
                        and j["communityschooldistrict"] == i["communityschooldistrict"] and j["congressionaldistrict"] == \
                        i["congressionaldistrict"] and j["incident_borough"] == i["congressionaldistrict"]:
                    i['district_id'] = j['ID']
            for j in self.alarm_boxes_df.to_dict('records'):
                if j["alarm_box_borough"] == i["alarm_box_borough"] and j["alarm_box_number"] == i["alarm_box_number"] and \
                        j["alarm_box_location"] == i["alarm_box_location"] \
                        and j["alarm_source_description_tx"] == i["alarm_source_description_tx"] and j[
                    "alarm_level_index_description"] == i["alarm_level_index_description"] and j["highest_alarm_level"] == \
                        i["highest_alarm_level"]:
                    i['alarm_box_id'] = j['ID']

        incidents_df = pd.DataFrame.from_dict(incidents)
        incidents_df.drop(['alarm_box_borough', 'alarm_box_number', 'alarm_box_location', 'alarm_source_description_tx',
                           'alarm_level_index_description', 'highest_alarm_level',
                           'zipcode', 'policeprecinct', 'citycouncildistrict', 'communitydistrict',
                           'communityschooldistrict', 'congressionaldistrict', 'incident_borough'], axis=1)
        return incidents_df

    def list_to_df(self, lst):
        df = pd.DataFrame.from_dict(lst)
        df = df.drop_duplicates(keep="first")
        df['ID'] = np.arange(1, len(df) + 1)
        return df

    # insert to tables
    def upload_to_db(self):
        #df.to_sql(tbl_name, db_file)
        # transforming lists to data frames
        self.classification_groups_df.to_sql('classification_groups', self.db_file)
        self.classifications_df.to_sql('classifications', self.db_file)
        self.locations_df.to_sql('district_info', self.db_file)
        self.alarm_boxes_df.to_sql('alarm_boxes', self.db_file)
        self.incidents_df.to_sql('fire_incidents', self.db_file)

e = FireIncidenceETL()








