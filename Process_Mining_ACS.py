# Process Mining Code
# Morteza Noshad;  Stanford
import numpy as np
import pandas as pd
from process_mining import generate_process_mining

#Big Query
from google.cloud import bigquery
from google.cloud.bigquery import dbapi;
client = bigquery.Client("mining-clinical-decisions"); # Project identifier
conn = dbapi.connect(client);
cursor = conn.cursor();
import google.auth

credentials, your_project_id = google.auth.default( scopes=["https://www.googleapis.com/auth/cloud-platform"])
bqclient = bigquery.Client(credentials=credentials, project="mining-clinical-decisions",)

query = "SELECT jc_uid as user_id, datetime_diff(access_time_jittered, emergencyAdmitTime, minute) as time, metric_name as event_name, event_type  FROM `mining-clinical-decisions.rose_team.cohort_AL` WHERE datetime_diff(access_time_jittered, emergencyAdmitTime, minute) < 1440"
dataframe = bqclient.query(query).result().to_dataframe()

# read the event log into dataframe
#dataframe = pd.read_csv('data_randomized.csv')
print('data loaded')


user_id_column ='user_id'
time_column = 'time'
event_label_columns = ['event_type', 'event_name'] # Event categories come first
types_to_include = ['ADT', 'Order Procedure', 'Order Medication', 'Lab Result',
 'Radiology Report', 'Medication Given']
include_event_list = [] 
filter_encoding_dict={'Order Procedure': 'OP', 'Lab Result':'LR', '2001002':'ER', 'ADT-':'', 'Started': 'Done', 'ALTEPLASE 100': 'ALTEPLASE (tPA) 100', 'OXYGEN:': 'OXYGEN'}
num_nodes = 12
edge_weight_lower_bound = 10


# plot the process mining graph
generate_process_mining(dataframe, user_id_column = user_id_column , time_column = time_column, 
	event_label_columns = event_label_columns , types_to_include = types_to_include, 
	filter_encoding_dict = filter_encoding_dict , num_nodes = num_nodes, edge_weight_lower_bound = edge_weight_lower_bound)
