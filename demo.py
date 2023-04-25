import requests
import json
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession with local mode, 2 threads, and an app name
spark = SparkSession.builder.master('local[2]').appName('assignment').getOrCreate()

# Function to get data from the COVID-19 API
def getData():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"
    headers = {
        "X-RapidAPI-Key": "8465bd3609msh0dbe4ba9a8ee5b6p122a32jsn67638c834968",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers)
    return json.loads(response.text)

# Function to trim state names
def trimState(state):
    # Find the index of '*' character in the state name
    idx = state.find('*')
    if idx != -1:
        # If '*' is found, return the state name without '*'
        return state[:idx]
    else:
        # If '*' is not found, return the original state name
        return state

# Function to clean the retrieved JSON data
def cleanData(jsonData):
    data = []
    for state in jsonData:
        l=[]
        dataValues = jsonData[state]
        if isinstance(dataValues, dict):
            for val in dataValues:
                l.append(dataValues[val])
            data.append(l)

    data.pop() # Remove the last element from data, as it contains total values
    for l in data:
        # Convert confirmed, cured, and death cases to integers
        l[2] = int(l[2])
        l[3] = int(l[3])
        l[1] = trimState(l[1]) # Trim the state name

    return data

# Function to create a DataFrame from cleaned data
def createDf(data):
    schema = StructType([
        StructField('Slno', IntegerType(), False),
        StructField('State', StringType(), False),
        StructField('Confirm', IntegerType(), True),
        StructField('Cured', IntegerType(), True),
        StructField('Death', IntegerType(), True),
        StructField('Total', IntegerType(), True)
    ])

    df = spark.createDataFrame(data=data, schema=schema)
    return df

# Get data from the COVID-19 API
jsonResponse = getData()	
# Clean the retrieved JSON data
data = cleanData(jsonResponse)
# Create a DataFrame from cleaned data
df = createDf(data)
