# Databricks notebook source
### code to create a mount blod to databricks
'''
dbutils.help()
dbutils.fs.help()

dbutils.fs.mount(source='wasbs://team4data@team4blobsa.blob.core.windows.net',
mount_point='/mnt/blobstorage',
extra_configs={'fs.azure.account.key.team4blobsa.blob.core.windows.net':'    '}
)

dbutils.fs.ls('/mnt/blobstorage')
'''


# COMMAND ----------

## Add necessary libraries
%pip install forex_python
%pip install forex_python pycountry
import pandas as pd
from datetime import datetime
from forex_python.converter import CurrencyRates
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
import pycountry

# COMMAND ----------

###Input all the required files from azure blob storage
df_Inbound = spark.read.option("header", "true").csv('/mnt/blobstorage/InboundData.csv')
df_Outbound_busi = spark.read.option("header", "true").csv('/mnt/blobstorage/Business_Merged.csv')
df_Outbound_plea = spark.read.option("header", "true").csv('/mnt/blobstorage/Pleasure_Merged.csv')
df_Outbound_stud = spark.read.option("header", "true").csv('/mnt/blobstorage/Student_Merged.csv')

# COMMAND ----------

## Initial data checks 
print('Inbound row count :',df_Inbound.count())
print('Outbound Business row count :',df_Outbound_busi.count())
print('Outbound Pleasure row count :',df_Outbound_plea.count())
print('Outbound Student row count :',df_Outbound_stud.count())
print('\nDescribe Inbound')
print(df_Inbound.describe().toPandas())
print('\nDescribe Outbound Business')
print(df_Outbound_busi.describe().toPandas())
print('\nDescribe Outbound Pleasure')
print(df_Outbound_plea.describe().toPandas())
print('\nDescribe Outbound Student')
print(df_Outbound_stud.describe().toPandas())

# COMMAND ----------

## display first 5 rows of all the dataframes
display(df_Inbound.limit(5))
display(df_Outbound_busi.limit(5))
display(df_Outbound_plea.limit(5))
display(df_Outbound_stud.limit(5))

# COMMAND ----------

df_Inbound = df_Inbound.dropDuplicates()
df_Outbound_busi = df_Outbound_busi.dropDuplicates()
df_Outbound_plea = df_Outbound_plea.dropDuplicates()
df_Outbound_stud = df_Outbound_stud.dropDuplicates()

# List of column names to keep (those that do not start with 'lkm')
columns_to_keep = [column for column in df_Inbound.columns if not column.startswith('_c')]

# Select only the columns to keep, effectively deleting the others
df_Inbound = df_Inbound.select(*columns_to_keep)

def format_column_name(column_name):
    return " ".join(word.capitalize() for word in column_name.replace('_', ' ').split())

# Apply the function to each column name and rename the columns in df_Inbound
for column in df_Inbound.columns:
    new_column_name = format_column_name(column)
    df_Inbound = df_Inbound.withColumnRenamed(column, new_column_name)

# Apply the function to each column name and rename the columns in df_Outbound_busi
for column in df_Outbound_busi.columns:
    new_column_name = format_column_name(column)
    df_Outbound_busi = df_Outbound_busi.withColumnRenamed(column, new_column_name)

# Apply the function to each column name and rename the columns in df_Outbound_plea
for column in df_Outbound_plea.columns:
    new_column_name = format_column_name(column)
    df_Outbound_plea = df_Outbound_plea.withColumnRenamed(column, new_column_name)

# Apply the function to each column name and rename the columns in df_Outbound_stud
for column in df_Outbound_stud.columns:
    new_column_name = format_column_name(column)
    df_Outbound_stud = df_Outbound_stud.withColumnRenamed(column, new_column_name)


df_Inbound = df_Inbound.withColumn('Travel Type', lit("Inbound"))
df_Outbound_busi = df_Outbound_busi.withColumn('Travel Type', lit("Outbound"))
df_Outbound_plea = df_Outbound_plea.withColumn('Travel Type', lit("Outbound"))
df_Outbound_stud = df_Outbound_stud.withColumn('Travel Type', lit("Outbound"))


# COMMAND ----------

# Rename multiple columns in df_Inbound
df_Inbound = df_Inbound.withColumnRenamed("Estimated Expense", "Estimated Expenses").withColumnRenamed("Transportion Mode", "Transportation Mode")
df_Outbound_stud = df_Outbound_stud.withColumnRenamed("Estimated Expense", "Estimated Expenses")

display(df_Inbound.limit(5))
display(df_Outbound_busi.limit(5))
display(df_Outbound_plea.limit(5))
display(df_Outbound_stud.limit(5))

# COMMAND ----------

print(df_Inbound.count())
print(df_Outbound_busi.count())
print(df_Outbound_plea.count())
print(df_Outbound_stud.count())

# COMMAND ----------

df_Inbound = df_Inbound.na.drop(subset=["First Name"])
print(df_Inbound.count())
df_Inbound=df_Inbound.withColumn('Id', df_Inbound['Id'] + 10000000)
df_Outbound_busi=df_Outbound_busi.withColumn('Id', df_Outbound_busi['Id'] + 100000)
df_Outbound_plea=df_Outbound_plea.withColumn('Id', df_Outbound_plea['Id'] + 1000000)
df_Outbound_stud=df_Outbound_stud.withColumn('Id', df_Outbound_stud['Id'] + 10000)
## 10023098
## 1008500
## 100990
## 10071

# COMMAND ----------

display(df_Inbound.limit(5))
display(df_Outbound_busi.limit(5))
display(df_Outbound_plea.limit(5))
display(df_Outbound_stud.limit(5))

# COMMAND ----------

# Convert the Spark DataFrames to pandas DataFrames
df_Inbound_pd = df_Inbound.toPandas()
df_Outbound_busi_pd = df_Outbound_busi.toPandas()
df_Outbound_plea_pd = df_Outbound_plea.toPandas()
df_Outbound_stud_pd = df_Outbound_stud.toPandas()
df = pd.concat([df_Inbound_pd, df_Outbound_busi_pd,df_Outbound_plea_pd,df_Outbound_stud_pd])
print(df.count())

# COMMAND ----------

# Count the number of rows in the pandas DataFrame 'df'
row_count, column_count = df.shape
print(f"Row count: {row_count}, Column count: {column_count}")

# COMMAND ----------

df.head(1)

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

## Trasformations of data
df['Gender'] = df['Gender'].replace('M', 'Male').replace('F', 'Female')
df['Age'] = df['Age'].astype(int)
df['Id'] = df['Id'].astype(int)
df['Entry Date'] = pd.to_datetime(df['Entry Date'])
df['Exit Date'] = pd.to_datetime(df['Exit Date'])
df['Start Date'] = pd.to_datetime(df['Start Date'])
df['End Date'] = pd.to_datetime(df['End Date'])
df['Symbol'] = df['Estimated Expenses'].str.extract('([^\d.]*)').replace('Â', '')
df['Amount'] = df['Estimated Expenses'].str.replace('[^0-9.]', '').astype(float)
df = df.rename(columns={'Destination Country': 'Country'})


# COMMAND ----------

df.head(2)

# COMMAND ----------

print(df.dtypes) 

# COMMAND ----------

# Fetch all exchange rates
c = CurrencyRates()
exchange_rates = c.get_rates('USD')

# Create an empty list to store converted amounts
converted_amounts = []

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    amount = row['Amount']
    symbol = row['Symbol']

    currencies = {
        '$': 'USD',
        '€': 'EUR',
        '£': 'GBP',
        '¥': 'JPY',
        '₹': 'INR'
        # Add more symbols and currency codes as needed
    }

    currency_code = currencies.get(symbol, 'Unknown')
    if currency_code == 'Unknown':
        print(f"Unable to guess the currency for amount {amount} {symbol}.")
        converted_amounts.append(None)
    else:
        if currency_code not in exchange_rates:
            
            converted_amounts.append(amount)
        else:
            exchange_rate = exchange_rates[currency_code]
            converted_amount = amount * exchange_rate
            converted_amounts.append(converted_amount)

# Add the converted amounts to the DataFrame
df['Estimated Expenses USD'] = converted_amounts
df['Estimated Expenses USD'] = df['Estimated Expenses USD'].map(lambda x: '{:.2f}'.format(x))
df['Modified Date'] = datetime.now().date()
# Display the DataFrame with the converted amounts
df.head()

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

import pycountry
def get_country_code_from_name(country_name):
    try:
        return pycountry.countries.lookup(country_name).alpha_2
    except LookupError:
        return None

# Suppose you have a dataframe with a column 'countryName' which contains country names

df['Country Code'] = df['Country'].apply(get_country_code_from_name)


# COMMAND ----------

df.head(1)

# COMMAND ----------


import calendar

# Define the age categorization function
def categorize_age_group(age):
    if age <= 17:
        return '0-17'
    elif age <= 30:
        return '18-30'
    elif age <= 45:
        return '31-45'
    elif age <= 60:
        return '46-60'
    else:
        return '61+'

# Apply the function to create a new column 'AgeGroup'
df['AgeGroup'] = df['Age'].apply(categorize_age_group)

# Convert 'Entry Date' to datetime to extract 'year' and 'month' as names
df['Entry Date'] = pd.to_datetime(df['Entry Date'])
df['year'] = df['Entry Date'].dt.year
df['month'] = df['Entry Date'].dt.month.apply(lambda x: calendar.month_name[x])

# Assuming the DataFrame contains data for a single country, extract the country code and name from the first row
country_code = df['Country Code'].iloc[0].lower()  # Ensure 'id' is lowercase
country_name = df['Country'].iloc[0]

# Starting the data structure with the country code (as 'id') and name
data_structure = {
    'id': country_code,
    'country_name': country_name,
    'years': []
}

# Populate the structure with year, month, state, and visa details
for year in df['year'].unique():
    year_dict = {'year': str(year), 'months': []}
    year_data = df[df['year'] == year]

    for month in year_data['month'].unique():
        month_dict = {'month': month, 'states': []}
        month_data = year_data[year_data['month'] == month]

        for state in month_data['Destination State'].unique():
            state_data = month_data[month_data['Destination State'] == state]
            state_dict = {
                'name': state,
                'total_arrivals': len(state_data),
                'visas': []
            }

            for visa_type, visa_group_df in state_data.groupby('Visa Type'):
                visa_details = {
                    'type': visa_type,
                    'total_arrivals': len(visa_group_df),
                    'AgeGroups': {},
                    'TransportationModes': {}
                }

                for age_group, age_group_df in visa_group_df.groupby('AgeGroup'):
                    visa_details['AgeGroups'][age_group] = len(age_group_df)

                for mode, mode_group_df in visa_group_df.groupby('Transportation Mode'):
                    visa_details['TransportationModes'][mode] = len(mode_group_df)

                state_dict['visas'].append(visa_details)

            month_dict['states'].append(state_dict)

        year_dict['months'].append(month_dict)

    data_structure['years'].append(year_dict)

# Displaying a snippet of the modified structure for verification, focusing on the first year
data_structure_snippet = {
    'id': data_structure['id'],
    'country_name': data_structure['country_name'],
    'years': data_structure['years'][:1]  # Display only the first year for brevity
}


# COMMAND ----------

df.head(1)

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

data_structure_snippet

# COMMAND ----------

from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id, to_json, struct
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import lit


# Convert the Python dictionary to JSON string
data_structure_json = json.dumps(data_structure)

# Convert JSON string to DataFrame
data_df = spark.read.json(spark.sparkContext.parallelize([data_structure_json]))




# COMMAND ----------

##Create spark dataframe 
spark_df = spark.createDataFrame(df)
# Write the Spark DataFrame to Azure Blob Storage as a CSV file
output_blob_path = "/mnt/blobstorage/Output"
spark_df.write.csv(output_blob_path, header=True, mode="overwrite")

# COMMAND ----------

## Load df in a document db 
## Install required packages
##%pip install azure-cosmos
##%pip install --upgrade pip

## Create connections
cosmosEndpoint = "https://team4docdb.documents.azure.com:443/"
cosmosMasterKey = "  "
cosmosDatabaseName = "Document DB"
cosmosContainerName = "Container"
## Create a configuration 
cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

# Configure Catalog Api    
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cfg["spark.cosmos.accountEndpoint"])
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cfg["spark.cosmos.accountKey"])

# Create Spark session

data_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()


# COMMAND ----------


# Serialize the JSON object to a string
json_string = json.dumps(data_structure, indent=4)

# Write the JSON string to a file
with open('data_structure.json', 'w') as file:
    file.write(json_string)

dbutils.fs.mv("file:/databricks/driver/data_structure.json", "dbfs:/FileStore/data_structure.json")

# COMMAND ----------

# Creating unique lists for each vertex type
df['Id'] = df['Id'].astype(str)
# Tourist vertices - Using unique combination of 'Id', 'First Name', and 'Last Name'

# Select unique rows based on 'Id' to ensure each tourist is represented once
unique_tourists = df.drop_duplicates(subset=['Id'])

# Concatenate 'First Name' and 'Last Name' to form the full name
unique_tourists['FullName'] = unique_tourists['First Name'] + ' ' + unique_tourists['Last Name']

# Create the tourist vertices DataFrame with 'id' and 'FullName'
tourist_vertices = unique_tourists[['Id', 'FullName']].rename(columns={'Id': 'id', 'FullName': 'name'})

# Visa vertices - Using unique 'Visa Type'
visa_vertices = df['Visa Type'].drop_duplicates()
visa_vertices = visa_vertices.rename("visa_type")


# Country vertices - Using unique 'Country'
country_vertices = df['Country'].drop_duplicates()
country_vertices = country_vertices.rename("country_name")
# Preparing the data for creating edges
# Holds relationship data (between Tourist and Visa)
holds_edges = df[['Id', 'Visa Type']].drop_duplicates()

# Offers relationship data (between Country and Visa)
# Assuming that each country offers all visa types present in the data
offers_edges = pd.DataFrame([(country_name,visa_type) for country_name in country_vertices for visa_type in visa_vertices],
                            columns=['Country', 'Visa Type'])

# Is From/Visits relationship data (between Tourist and Country)
# Assuming that a tourist 'visits' from the country of their nationality
is_from_visits_edges = df[['Id', 'Country']].drop_duplicates()

##Create vertices dataframe
# Visa vertices - Using unique 'Visa Type'
visa_vertices_1 = pd.DataFrame(df['Visa Type'].drop_duplicates().reset_index(drop=True))
visa_vertices_1.index += 1  # Start the index from 1 instead of 0
visa_vertices_1['id'] = visa_vertices_1.index.astype(str)
visa_vertices_1 = visa_vertices_1.rename(columns={'Visa Type': 'name'})

# Country vertices - Using unique 'Country'
country_vertices_1 = pd.DataFrame(df['Country'].drop_duplicates().reset_index(drop=True))
country_vertices_1.index += 1  # Start the index from 1 instead of 0
country_vertices_1['id'] = country_vertices_1.index.astype(str)
country_vertices_1 = country_vertices_1.rename(columns={'Country': 'name'})

vertices_pd = pd.concat([tourist_vertices, visa_vertices_1, country_vertices_1], ignore_index=True)


tourist_vertices, visa_vertices, country_vertices, holds_edges, offers_edges, is_from_visits_edges


# COMMAND ----------

pd.set_option('display.max_rows', None)
'''
print(tourist_vertices.head(2))
print(visa_vertices.head(2))
print(country_vertices.head(2))
print(vertices.head(2))
vertices.tail(12)
offers_edges.tail(10)
'''
vertices_pd.tail(12)

# COMMAND ----------

tourist_vertices.columns.tolist()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from graphframes import GraphFrame

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("Graph Data Model").getOrCreate()

# Convert the vertices and edges to Spark DataFrames
vertices = spark.createDataFrame(vertices_pd)
visa_vertices_df = spark.createDataFrame(visa_vertices.to_frame(name='VisaType'))
country_vertices_df = spark.createDataFrame(country_vertices.to_frame(name='Country'))
holds_edges_df = spark.createDataFrame(holds_edges)
offers_edges_df = spark.createDataFrame(offers_edges)
is_from_visits_edges_df = spark.createDataFrame(is_from_visits_edges)

# Create an edges DataFrame with src and dst columns
edges = (holds_edges_df
         .withColumnRenamed("Id", "src")
         .withColumnRenamed("Visa Type", "dst")
         .unionByName(offers_edges_df.withColumnRenamed("Country", "src")
                                     .withColumnRenamed("Visa Type", "dst"))
         .unionByName(is_from_visits_edges_df.withColumnRenamed("Id", "src")
                                             .withColumnRenamed("Country", "dst")))

edges =edges.withColumnRenamed("src", "src")

# Create a GraphFrame
graph = GraphFrame(vertices, edges)

# Now you can run graph algorithms, for example finding all the trips for a certain tourist
# Here's an example of running a simple filter operation to find all vertices representing tourists.
tourists = graph.vertices.filter(col("id").cast("string").like("1000%"))

# Display the result
tourists.show()


# COMMAND ----------

edges = edges.withColumn('id', col('src'))
edges.show()

# COMMAND ----------


# Cosmos DB configuration
config = {
    "spark.cosmos.accountEndpoint": "https://team4graphdb.documents.azure.com:443/",
    "spark.cosmos.accountKey": "     ",
    "spark.cosmos.database": "Team4GraphDB",
    "spark.cosmos.container": "Graph1",  # This should be your graph container
}

# Initialize Spark Session with Cosmos DB configuration
spark = SparkSession.builder.appName("CosmosDBGraph") \
    .config("spark.jars.packages", "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.2.0") \
    .config("spark.cosmos.accountEndpoint", config["spark.cosmos.accountEndpoint"]) \
    .config("spark.cosmos.accountKey", config["spark.cosmos.accountKey"]) \
    .config("spark.cosmos.database", config["spark.cosmos.database"]) \
    .getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import col, when

vertices.printSchema()

# Check the first few rows to ensure they look as expected
vertices.show()

# Write vertices DataFrame to Cosmos DB
vertices.write.format("cosmos.oltp") \
    .options(**config) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .option("spark.cosmos.write.bulk.enabled", "true") \
    .mode("Append") \
    .save()

# COMMAND ----------


from pyspark.sql.functions import monotonically_increasing_id


# If an 'id' column does not exist, create one with unique values
if "id" not in edges.columns:
    edges = edges.withColumn("id", monotonically_increasing_id().cast("string"))

# If 'id' exists but is not of string type, cast it to string
else:
    edges = edges.withColumn("id", col("id").cast("string"))

# Now write the edges DataFrame to Cosmos DB
edges.write.format("cosmos.oltp") \
    .options(**config) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .option("spark.cosmos.write.bulk.enabled", "true") \
    .mode("Append") \
    .save()


# COMMAND ----------

vertices.show()

# COMMAND ----------

edges.show()

# COMMAND ----------

vertices.count()
edges.count()

# COMMAND ----------


# Cosmos DB configuration
config1 = {
    "spark.cosmos.accountEndpoint": "https://team4graphdb.documents.azure.com:443/",
    "spark.cosmos.accountKey": "  ",
    "spark.cosmos.database": "Team4GraphDB",
    "spark.cosmos.container": "Graph2",  # This should be your graph container
}

# Initialize Spark Session with Cosmos DB configuration
spark = SparkSession.builder.appName("CosmosDBGraph") \
    .config("spark.jars.packages", "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.2.0") \
    .config("spark.cosmos.accountEndpoint", config["spark.cosmos.accountEndpoint"]) \
    .config("spark.cosmos.accountKey", config["spark.cosmos.accountKey"]) \
    .config("spark.cosmos.database", config["spark.cosmos.database"]) \
    .getOrCreate()


from pyspark.sql.functions import monotonically_increasing_id

vertices.write.format("cosmos.oltp") \
    .options(**config) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .option("spark.cosmos.write.bulk.enabled", "true") \
    .mode("Append") \
    .save()
# If an 'id' column does not exist, create one with unique values
if "id" not in edges.columns:
    edges = edges.withColumn("id", monotonically_increasing_id().cast("string"))

# If 'id' exists but is not of string type, cast it to string
else:
    edges = edges.withColumn("id", col("id").cast("string"))

# Now write the edges DataFrame to Cosmos DB
edges.write.format("cosmos.oltp") \
    .options(**config1) \
    .option("spark.cosmos.write.strategy", "ItemOverwrite") \
    .option("spark.cosmos.write.bulk.enabled", "true") \
    .mode("Append") \
    .save()
