import pandas as pd 
from pymongo import MongoClient
import os
#ETL / ELT Pipeline for Employee Data
#Extract - csv to pandas DataFrame
emp_df = pd.read_csv(os.path.join(os.path.dirname(__file__), 'hpr.csv'))

#Load - lake pandas DataFrame to MongoDB
client = MongoClient('mongodb+srv://sonucharanyat:FdjnDa09MdFDJZrA@cluster0.3yj8zq2.mongodb.net/')
db = client['lake_emp']
collection = db['hprs']
collection.delete_many({})
collection.insert_many(emp_df.to_dict('records'))
print('Patients loaded to lake database')

#Transform - MongoDB to pandas DataFrame
emp_df['LabResult_Hb'] = emp_df['LabResult_Hb'].fillna(0)
dept_sal_df = emp_df.groupby('Medication')['LabResult_Hb'].sum().reset_index()

#Load - warehouse pandas DataFrame to MongoDB
db = client['warehouse_hpr']
collection = db['hprs']
collection.delete_many({})
collection.insert_many(emp_df.to_dict('records'))
print('Processed Patients loaded to warehouse database')
collection = db['dept_salaries']
collection.delete_many({})
collection.insert_many(dept_sal_df.to_dict('records'))
print('Processed Daigonis Lab result loaded to warehouse database')