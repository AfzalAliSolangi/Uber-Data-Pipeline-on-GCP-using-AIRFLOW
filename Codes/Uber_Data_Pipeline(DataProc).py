#!/usr/bin/env python
# coding: utf-8

# In[57]:


import io
import pandas as pd
import requests
from google.cloud import storage,bigquery


# In[58]:


client = bigquery.Client(project = 'academic-pier-405912')


# In[59]:


bucket_name = 'afzal03082821k407711123'
file_name = 'raw_data/uber_data.csv'  # Replace with the actual file path in the bucket.
df = pd.read_csv('gs://' + bucket_name + '/' + file_name, encoding='utf-8', sep=',')


# In[60]:


df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])


# In[61]:


df = df.drop_duplicates().reset_index(drop=True)
df['trip_id'] = df.index


# In[62]:


#Datetime_dimension
datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday


datetime_dim['datetime_id'] = datetime_dim.index

# datetime_dim = datetime_dim.rename(columns={'tpep_pickup_datetime': 'datetime_id'}).reset_index(drop=True)
datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                             'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]


# In[63]:


# Datetime_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.datetime_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    datetime_dim,table_id,job_config=job_config
)
job.result()


# In[64]:


#Passenger_count_dimension
passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]


# In[65]:


#Passenger_count_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.passenger_count_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    passenger_count_dim,table_id,job_config=job_config
)
job.result()


# In[66]:


#Trip_Distance_dimension
trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]


# In[67]:


#Trip_Distance_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.trip_distance_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    trip_distance_dim,table_id,job_config=job_config
)
job.result()


# In[68]:


#Rate_Code_dimension
rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
}

rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
rate_code_dim['rate_code_id'] = rate_code_dim.index
rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]


# In[69]:


#Rate_Code_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.rate_code_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    rate_code_dim,table_id,job_config=job_config
)
job.result()


# In[79]:


#Pickup_Location_dimension
pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 


# In[80]:


#Pickup_Location_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.pickup_location_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    pickup_location_dim,table_id,job_config=job_config
)
job.result()


# In[75]:


#Dropoff_Location_dimension
dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]


# In[76]:


#Dropoff_Location_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.dropoff_location_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    dropoff_location_dim,table_id,job_config=job_config
)
job.result()


# In[77]:


#Payment_type_dimension
payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
}
payment_type_dim = df[['payment_type']].reset_index(drop=True)
payment_type_dim['payment_type_id'] = payment_type_dim.index
payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]


# In[78]:


#Payment_type_dimension load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.payment_type_dim'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    payment_type_dim,table_id,job_config=job_config
)
job.result()


# In[ ]:


#Fact Table
fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
             .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
             .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]


# In[71]:


#Fact Table load to BigQuery
table_id = 'academic-pier-405912.Uber_data_21k4077.fact_table'
# Specify the output table in the format "project_id.dataset_id.table_id"

job_config = bigquery.LoadJobConfig(
write_disposition = 'WRITE_TRUNCATE')

job = client.load_table_from_dataframe(
    fact_table,table_id,job_config=job_config
)
job.result()


# In[72]:


# CREATE OR REPLACE TABLE `data-with-darshil.uber_dataset.tbl_analysis_report` AS (
# SELECT
#   f.VendorID,
#   f.tpep_pickup_datetime,
#   f.tpep_dropoff_datetime,
#   p.passenger_count,
#   td.trip_distance,
#   rc.RatecodeID,
#   f.store_and_fwd_flag,
#   pl.pickup_latitude,
#   pl.pickup_longitude,
#   dl.dropoff_latitude,
#   dl.dropoff_longitude,
#   pt.payment_type,
#   f.fare_amount,
#   f.extra,
#   f.mta_tax,
#   f.tip_amount,
#   f.tolls_amount,
#   f.improvement_surcharge,
#   f.total_amount
# FROM
#   `data-with-darshil.uber_dataset.fact_table` f
#   JOIN `data-with-darshil.uber_dataset.passenger_count_dim` p ON f.passenger_count_id = p.passenger_count_id
#   JOIN `data-with-darshil.uber_dataset.trip_distance_dim` td ON f.trip_distance_id = td.trip_distance_id
#   JOIN `data-with-darshil.uber_dataset.rate_code_dim` rc ON f.rate_code_id = rc.rate_code_id
#   JOIN `data-with-darshil.uber_dataset.pickup_location_dim` pl ON f.pickup_location_id = pl.pickup_location_id
#   JOIN `data-with-darshil.uber_dataset.dropoff_location_dim` dl ON f.dropoff_location_id = dl.dropoff_location_id
#   JOIN `data-with-darshil.uber_dataset.payment_type_dim` pt ON f.payment_type_id = pt.payment_type_id);


# In[ ]:




