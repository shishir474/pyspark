import requests
import json
from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.master('local[2]').appName('assignment').getOrCreate()

def getData():
	url = "https://covid-19-india2.p.rapidapi.com/details.php"
	headers = {
		"X-RapidAPI-Key": "8465bd3609msh0dbe4ba9a8ee5b6p122a32jsn67638c834968",
		"X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
	}
	response = requests.request("GET", url, headers=headers)
	return json.loads(response.text)


def trimState(state):
	idx = state.find('*')
	if idx != -1:
		return state[:idx]
	else:
		return state
	

def cleanData(jsonData):
	data = []
	for state in jsonData:
		l=[]
		dataValues = jsonData[state]
		if isinstance(dataValues, dict):
			for val in dataValues:
				l.append(dataValues[val])
			data.append(l)

	data.pop()
	for l in data:
		l[2] = int(l[2])
		l[3] = int(l[3])
		l[1] = trimState(l[1])
	

	return data


def createDf(data):
	schema = StructType([
		StructField('Slno',IntegerType(),False),
		StructField('State', StringType(), False),
		StructField('Confirm',IntegerType(),True),
		StructField('Cured', IntegerType(), True),
		StructField('Death', IntegerType(), True),
		StructField('Total', IntegerType(), True)
	])

	df = spark.createDataFrame(data=data, schema=schema)
	return df


jsonResponse = getData()	
data = cleanData(jsonResponse)
df = createDf(data)





















