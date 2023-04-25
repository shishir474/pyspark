# pyspark assignment:

# Demo.py
This code is a Python script that fetches COVID-19 data from an API for different states in India, cleans the retrieved data, 
and creates a PySpark DataFrame for further data analysis. Let's go through the main components of the code:

Importing dependencies:
The script imports the following dependencies:
requests: for making HTTP requests to the API.
json: for parsing the JSON response from the API.
pprint: for pretty-printing JSON data for better readability.
pyspark: for performing data analysis using PySpark.
SparkSession: for creating and managing the Spark session.
StructType, StructField, StringType, IntegerType: for defining the schema of the PySpark DataFrame.

Creating a SparkSession:
The script creates a SparkSession with local mode, 2 threads, and an app name using the SparkSession class from PySpark. 
The SparkSession is the entry point for using PySpark and allows the script to interact with Spark for distributed data processing.

Retrieving data from the COVID-19 API:
The getData() function makes a GET request to the COVID-19 API using the requests library.
It includes the required headers with the RapidAPI key and host for authentication. The response is in JSON format.

Cleaning the retrieved data:
The cleanData() function takes the retrieved JSON data and cleans it. It iterates through the states in the JSON data and extracts 
the required values. It trims the state names by removing any trailing '*' character and converts the confirmed, cured, and death cases to integers.

Creating a PySpark DataFrame:
The createDf() function takes the cleaned data and creates a PySpark DataFrame. It defines the schema of the DataFrame using 
StructType, StructField, StringType, and IntegerType. The DataFrame is created using the spark.createDataFrame() method with 
the defined schema and the cleaned data.

Executing the functions:
The main part of the code calls the functions in the following order:

getData() to fetch COVID-19 data from the API and store it in jsonResponse.
cleanData() to clean the retrieved JSON data and store it in data.
createDf() to create a PySpark DataFrame from the cleaned data and store it in df.

#app.py
In app.py created a Flask app and inside it created the routes for all the queries used in demo.py and retured the output in json format.
