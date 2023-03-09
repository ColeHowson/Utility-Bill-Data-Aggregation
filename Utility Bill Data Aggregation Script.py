# Import Required Libraries 
import pandas as pd 
import numpy as np
import pyodbc
pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', None)

#Create Connection String 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER = *****; DATABASE= *****; UID = *****; PWD = *****')
#Create cursor object to execute SQL query 
cursor = cnxn.cursor()
#SQL Query for  College Electricity Consumption 
sql = "SELECT * FROM"
#Executing query and storing it in the varible 'data'
data = cursor.execute(sql).fetchall()
columns = [column[0] for column in cursor.description]
#Closing connecton to the database 
cnxn.close()
#Converting the returned data from a list of tuples to a list using list comprehension 
lst = [list(row) for row in data]
#using pandas to convert list into a Panadas Dataframe object 
df = pd.DataFrame(lst)
df.columns = columns 

#Filtering Specific Location Data

Royal_Oak_df = df[(df['Address'] == '*****') & (df['CategoryType'] == 3)]
Royal_Oak_df['StartDate'] = pd.to_datetime(Royal_Oak_df['StartDate'])
Royal_Oak_df['EndDate'] = pd.to_datetime(Royal_Oak_df['EndDate'])
Royal_Oak_df['Value'] = Royal_Oak_df['Value'].astype(float)
Royal_Oak_df = Royal_Oak_df[(Royal_Oak_df['StartDate'] >= '2018-12-01') & (Royal_Oak_df['EndDate'] < '2023-02-01') & (Royal_Oak_df['CategoryDescription'] != 'Taxes')]
Royal_Oak_df = Royal_Oak_df[['AccountNo','StartDate','EndDate','LineDescription','CategoryDescription','Value','Unit','Commodity']]


##### Electricity #####
Royal_Oak_Elec_df = Royal_Oak_df[Royal_Oak_df['Commodity'] == 'Electricity']

#Creating Month Buckets 

#Rounds the StartDate down to be the beginning of the month
Royal_Oak_Elec_df ['MONTH_1'] = pd.to_datetime(Royal_Oak_Elec_df .StartDate.values.astype('datetime64[M]'))

#Creates Month 2 by taking the rounded down Month 1 and adding one month
Royal_Oak_Elec_df ['MONTH_2'] = pd.to_datetime(Royal_Oak_Elec_df .MONTH_1.values.astype('datetime64[M]') + 1)

#Calculating the number of bill days per billing period(+1 makes sure the last day included) 
Royal_Oak_Elec_df ['BillDays'] = (Royal_Oak_Elec_df ['EndDate'] - Royal_Oak_Elec_df ['StartDate']).dt.days + 1

#Determining the amount of days from a billing period belong in each month bucket
Royal_Oak_Elec_df ['MONTH_DAYS_1'] = (Royal_Oak_Elec_df ['MONTH_2'] - Royal_Oak_Elec_df ['StartDate'] - np.timedelta64(0,'D')).astype('timedelta64[D]')
Royal_Oak_Elec_df ['MONTH_DAYS_2'] = Royal_Oak_Elec_df ['BillDays'] - Royal_Oak_Elec_df ['MONTH_DAYS_1']

Royal_Oak_Elec_df['MONTH_DAYS_2'] = Royal_Oak_Elec_df['MONTH_DAYS_2'].apply(lambda x: 0 if x < 0 else x )

#Calculating the percentage of Cost/Consumption for each month
Royal_Oak_Elec_df ['Charges_M1'] = round(Royal_Oak_Elec_df ['Value'] * Royal_Oak_Elec_df ['MONTH_DAYS_1'] / Royal_Oak_Elec_df ['BillDays'],2)
Royal_Oak_Elec_df ['Charges_M2'] = round(Royal_Oak_Elec_df ['Value'] * Royal_Oak_Elec_df ['MONTH_DAYS_2'] / Royal_Oak_Elec_df ['BillDays'],2)


#Assigning Each Month to it's own DF
month_1_df = Royal_Oak_Elec_df.filter(['AccountNo','MONTH_1','MONTH_DAYS_1','Charges_M1','Commodity','Unit'])
month_2_df = Royal_Oak_Elec_df.filter(['AccountNo','MONTH_2','MONTH_DAYS_2','Charges_M2','Commodity','Unit'])

#Renamning Both Frame's Columns Prior to Concatinating 
month_1_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']
month_2_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']

#Concating DFs
frames = (month_1_df,month_2_df)
Royal_Oak_Elec_df = pd.concat(frames)


#Aggregating DF 
Royal_Oak_Elec_df = Royal_Oak_Elec_df.groupby(['AccountNo','Date','Commodity','Unit']).sum().reset_index()
Royal_Oak_Elec_df.drop(columns = 'Month_Days',inplace = True )

# Pivoting Data/Renaming Columns
pivot_table = pd.pivot_table(Royal_Oak_Elec_df, index = ['AccountNo','Date','Commodity'],values = 'Charges', columns= 'Unit', aggfunc= sum)
pivot_table = pivot_table.reset_index()
Royal_Oak_Elec_df = pivot_table

Royal_Oak_Elec_df.columns = ['AccountNo','Date','Commodity','Cost(CAD)','Consumption(kWh)']

##### Water #####

Royal_Oak_Water_df = Royal_Oak_df[Royal_Oak_df['Commodity'] == 'Water']


#Creating Month Buckets 

#Rounds the StartDate down to be the beginning of the month
Royal_Oak_Water_df['MONTH_1'] = pd.to_datetime(Royal_Oak_Water_df.StartDate.values.astype('datetime64[M]'))

#Creates Month 2 by taking the rounded down Month 1 and adding one month
Royal_Oak_Water_df['MONTH_2'] = pd.to_datetime(Royal_Oak_Water_df.MONTH_1.values.astype('datetime64[M]') + 1)


#Calculating the number of bill days per billing period(+1 makes sure the last day included) 
Royal_Oak_Water_df['BillDays'] = (Royal_Oak_Water_df['EndDate'] - Royal_Oak_Water_df['StartDate']).dt.days + 1

#Determining the amount of days from a billing period belong in each month bucket
Royal_Oak_Water_df['MONTH_DAYS_1'] = (Royal_Oak_Water_df['MONTH_2'] - Royal_Oak_Water_df['StartDate'] - np.timedelta64(0,'D')).astype('timedelta64[D]')
Royal_Oak_Water_df['MONTH_DAYS_2'] = Royal_Oak_Water_df['BillDays'] - Royal_Oak_Water_df['MONTH_DAYS_1']

#If a bill starts in a month and ends prior to the end of the month ie: 2019-04-01 - 2019-04-22 these two lines will account for that
Royal_Oak_Water_df['MONTH_DAYS_1'] = np.where(Royal_Oak_Water_df['MONTH_DAYS_1'] > Royal_Oak_Water_df['BillDays'],Royal_Oak_Water_df['BillDays'],Royal_Oak_Water_df['MONTH_DAYS_1'])


Royal_Oak_Water_df['MONTH_DAYS_2'] = Royal_Oak_Water_df['MONTH_DAYS_2'].apply(lambda x: 0 if x < 0 else x)


#Calculating the percentage of Cost/Consumption for each month

Royal_Oak_Water_df['Charges_M1'] = round(Royal_Oak_Water_df['Value'] * Royal_Oak_Water_df['MONTH_DAYS_1'] / Royal_Oak_Water_df['BillDays'],2)
Royal_Oak_Water_df['Charges_M2'] = round(Royal_Oak_Water_df['Value'] * Royal_Oak_Water_df['MONTH_DAYS_2'] / Royal_Oak_Water_df['BillDays'],2)


#Assigning Each Month to it's own DF
month_1_df = Royal_Oak_Water_df.filter(['AccountNo','MONTH_1','MONTH_DAYS_1','Charges_M1','Commodity','Unit'])
month_2_df = Royal_Oak_Water_df.filter(['AccountNo','MONTH_2','MONTH_DAYS_2','Charges_M2','Commodity','Unit'])

#Renamning Both Frame's Columns Prior to Concatinating 
month_1_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']
month_2_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']

#Concating DFs
frames = (month_1_df,month_2_df)
Royal_Oak_Water_df = pd.concat(frames)

#Aggregating DF 
Royal_Oak_Water_df = Royal_Oak_Water_df.groupby(['AccountNo','Date','Commodity','Unit']).sum().reset_index()
Royal_Oak_Water_df.drop(columns = 'Month_Days',inplace = True )

Royal_Oak_Water_df.Unit = Royal_Oak_Water_df.Unit.apply(lambda x: "M3" if x == 'CM' else x)

# Pivoting Data/Renaming Columns
pivot_table = pd.pivot_table(Royal_Oak_Water_df, index = ['AccountNo','Date','Commodity'],values = 'Charges', columns= 'Unit', aggfunc= sum)
pivot_table = pivot_table.reset_index()
Royal_Oak_Water_df = pivot_table

Royal_Oak_Water_df.columns = ['AccountNo','Date','Commodity','Cost(CAD)','Consumption(M3)']

##### Sewer #####

Royal_Oak_Sewer_df = Royal_Oak_df[Royal_Oak_df['Commodity'] == 'Sewer']


#Creating Month Buckets 

#Rounds the StartDate down to be the beginning of the month
Royal_Oak_Sewer_df['MONTH_1'] = pd.to_datetime(Royal_Oak_Sewer_df.StartDate.values.astype('datetime64[M]'))

#Creates Month 2 by taking the rounded down Month 1 and adding one month
Royal_Oak_Sewer_df['MONTH_2'] = pd.to_datetime(Royal_Oak_Sewer_df.MONTH_1.values.astype('datetime64[M]') + 1)

#Calculating the number of bill days per billing period(+1 makes sure the last day included) 
Royal_Oak_Sewer_df['BillDays'] = (Royal_Oak_Sewer_df['EndDate'] - Royal_Oak_Sewer_df['StartDate']).dt.days + 1

#Determining the amount of days from a billing period belong in each month bucket
Royal_Oak_Sewer_df['MONTH_DAYS_1'] = (Royal_Oak_Sewer_df['MONTH_2'] - Royal_Oak_Sewer_df['StartDate'] - np.timedelta64(0,'D')).astype('timedelta64[D]')
Royal_Oak_Sewer_df['MONTH_DAYS_2'] = Royal_Oak_Sewer_df['BillDays'] - Royal_Oak_Sewer_df['MONTH_DAYS_1']

Royal_Oak_Sewer_df['MONTH_DAYS_2'] = Royal_Oak_Sewer_df['MONTH_DAYS_2'].apply(lambda x: 0 if x < 0 else x )

#Calculating the percentage of Cost/Consumption for each month
Royal_Oak_Sewer_df['Charges_M1'] = round(Royal_Oak_Sewer_df['Value'] * Royal_Oak_Sewer_df['MONTH_DAYS_1'] / Royal_Oak_Sewer_df['BillDays'],2)
Royal_Oak_Sewer_df['Charges_M2'] = round(Royal_Oak_Sewer_df['Value'] * Royal_Oak_Sewer_df['MONTH_DAYS_2'] / Royal_Oak_Sewer_df['BillDays'],2)

#Assigning Each Month to it's own DF
month_1_df = Royal_Oak_Sewer_df.filter(['AccountNo','MONTH_1','MONTH_DAYS_1','Charges_M1','Commodity','Unit'])
month_2_df = Royal_Oak_Sewer_df.filter(['AccountNo','MONTH_2','MONTH_DAYS_2','Charges_M2','Commodity','Unit'])

#Renamning Both Frame's Columns Prior to Concatinating 
month_1_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']
month_2_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']

#Concating DFs
frames = (month_1_df,month_2_df)
Royal_Oak_Sewer_df = pd.concat(frames)

#Aggregating DF 
Royal_Oak_Sewer_df = Royal_Oak_Sewer_df.groupby(['AccountNo','Date','Commodity','Unit']).sum().reset_index()
Royal_Oak_Sewer_df.drop(columns = 'Month_Days',inplace = True )

# Pivoting Data/Renaming Columns
pivot_table = pd.pivot_table(Royal_Oak_Sewer_df, index = ['AccountNo','Date','Commodity'],values = 'Charges', columns= 'Unit', aggfunc= sum)
pivot_table = pivot_table.reset_index()
Royal_Oak_Sewer_df= pivot_table

Royal_Oak_Sewer_df.columns = ['AccountNo','Date','Commodity','Cost(CAD)','Consumption(M3)']

##### Storm Water #####

Royal_Oak_Storm_df = Royal_Oak_df[Royal_Oak_df['Commodity'] == 'Storm Water']


#Creating Month Buckets 

#Rounds the StartDate down to be the beginning of the month
Royal_Oak_Storm_df['MONTH_1'] = pd.to_datetime(Royal_Oak_Storm_df.StartDate.values.astype('datetime64[M]'))

#Creates Month 2 by taking the rounded down Month 1 and adding one month
Royal_Oak_Storm_df['MONTH_2'] = pd.to_datetime(Royal_Oak_Storm_df.MONTH_1.values.astype('datetime64[M]') + 1)

#Calculating the number of bill days per billing period(+1 makes sure the last day included) 
Royal_Oak_Storm_df['BillDays'] = (Royal_Oak_Storm_df['EndDate'] - Royal_Oak_Storm_df['StartDate']).dt.days + 1

#Determining the amount of days from a billing period belong in each month bucket
Royal_Oak_Storm_df['MONTH_DAYS_1'] = (Royal_Oak_Storm_df['MONTH_2'] - Royal_Oak_Storm_df['StartDate'] - np.timedelta64(0,'D')).astype('timedelta64[D]')
Royal_Oak_Storm_df['MONTH_DAYS_2'] = Royal_Oak_Storm_df['BillDays'] - Royal_Oak_Storm_df['MONTH_DAYS_1']

Royal_Oak_Storm_df['MONTH_DAYS_2'] = Royal_Oak_Storm_df['MONTH_DAYS_2'].apply(lambda x: 0 if x < 0 else x )

#Calculating the percentage of Cost/Consumption for each month
Royal_Oak_Storm_df['Charges_M1'] = round(Royal_Oak_Storm_df['Value'] * Royal_Oak_Storm_df['MONTH_DAYS_1'] / Royal_Oak_Storm_df['BillDays'],2)
Royal_Oak_Storm_df['Charges_M2'] = round(Royal_Oak_Storm_df['Value'] * Royal_Oak_Storm_df['MONTH_DAYS_2'] / Royal_Oak_Storm_df['BillDays'],2)


#Assigning Each Month to it's own DF
month_1_df = Royal_Oak_Storm_df.filter(['AccountNo','MONTH_1','MONTH_DAYS_1','Charges_M1','Commodity','Unit'])
month_2_df = Royal_Oak_Storm_df.filter(['AccountNo','MONTH_2','MONTH_DAYS_2','Charges_M2','Commodity','Unit'])

#Renamning Both Frame's Columns Prior to Concatinating 
month_1_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']
month_2_df.columns = ['AccountNo','Date','Month_Days','Charges','Commodity','Unit']

#Concating DFs
frames = (month_1_df,month_2_df)
Royal_Oak_Storm_df = pd.concat(frames)


#Aggregating DF 
Royal_Oak_Storm_df = Royal_Oak_Storm_df .groupby(['AccountNo','Date','Commodity','Unit']).sum().reset_index()
Royal_Oak_Storm_df.drop(columns = 'Month_Days',inplace = True )

# Pivoting Data/Renaming Columns
pivot_table = pd.pivot_table(Royal_Oak_Storm_df, index = ['AccountNo','Date','Commodity'],values = 'Charges', columns= 'Unit', aggfunc= sum)
pivot_table = pivot_table.reset_index()
Royal_Oak_Storm_df = pivot_table

Royal_Oak_Storm_df.columns = ['AccountNo','Date','Commodity','Cost(CAD)']

##### Export Data to Excel #####

with pd.ExcelWriter(r'C:\Users\chowson\Desktop\PROJECTS\FCR Data Pull\OUTPUT\FCR_Royal_Oak_Centre.xlsx') as writer:
   Royal_Oak_Elec_df.to_excel(writer,sheet_name ='Electricity', index = False)
   Royal_Oak_Water_df.to_excel(writer,sheet_name ='Water', index = False)
   Royal_Oak_Sewer_df.to_excel(writer,sheet_name = 'Sewer', index = False)
   Royal_Oak_Storm_df.to_excel(writer,sheet_name = 'Storm Water', index = False)
