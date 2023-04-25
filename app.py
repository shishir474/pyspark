from flask import Flask, jsonify
from demo import df, spark  # Importing necessary modules and variables

app = Flask(__name__)  # Creating a Flask web application instance

@app.route('/')  # Defining the home route
def home():
    return jsonify({
        '/most_affected_state': "Most affected state among all the states ( total death/total covid cases)",
        '/least_affected_state': "Least affected state among all the states ( total death/total covid cases)",
        '/highest_covid_cases': "State with highest covid cases.",
        '/least_covid_cases': "State with least covid cases.",
        '/total_cases': "Total cases.",
        '/most_efficient_state': "State that handled the covid most efficiently( total recovery/ total covid cases).",
        '/least_efficient_state': "State that handled the covid least efficiently( total recovery/ total covid cases).",
        '/show_all_data': "Show all data"
    })  # Returning a JSON response with information about available routes

df.createOrReplaceTempView('table')  # Creating a temporary view of the DataFrame as 'table'

@app.route('/ShowMostAffectedState')  # Defining a route to get the most affected state
def getMostAffectedState():
    ans = spark.sql('SELECT State, Confirm/Death as ans FROM table ORDER BY ans DESC LIMIT 1').select('State').collect()[0][0]
    return jsonify({
        'most_affected_state': ans
    })  # Returning a JSON response with the most affected state

@app.route('/LeastAffectedState')  # Defining a route to get the least affected state
def getLeastAffectedState():
    ans = spark.sql('SELECT State, Confirm/Death as ans FROM table ORDER BY ans limit 1').select('State').collect()[0][0]
    return jsonify({
        'most_affected_state': ans
    })  # Returning a JSON response with the least affected state

@app.route('/StateWithMaxCovidCases')  # Defining a route to get the state with maximum COVID cases
def getStateWithMaxCovidCases():
    ans = spark.sql("Select State FROM table ORDER BY Confirm DESC LIMIT 1").collect()[0][0]
    return jsonify({
        'State_with_max_covid_cases': ans
    })  # Returning a JSON response with the state with maximum COVID cases

@app.route('/StateWithMinCovidCases')  # Defining a route to get the state with minimum COVID cases
def getStateWithMinCovidCases():
    ans = spark.sql("Select State FROM table ORDER BY Confirm LIMIT 1").collect()[0][0]
    return jsonify({
        'State_with_min_covid_cases': ans
    })  # Returning a JSON response with the state with minimum COVID cases

@app.route('/GetTotalCases')  # Defining a route to get the total COVID cases
def getTotalCases():
    ans = spark.sql("SELECT SUM(Confirm) as Total_cases FROM table").collect()[0][0]
    return jsonify({
        'Total_cases': ans
    })  # Returning a JSON response with the total COVID cases

@app.route("/GetMostEfficientState")  # Defining a route to get the most efficient state in handling COVID cases
def getMostEfficientState():
    ans = spark.sql("SELECT State, Cured/Total as ans FROM table ORDER BY ans DESC LIMIT 1").collect()[0][0]
    return jsonify({
        'Most_efficient_state': ans
    })  # Returning a JSON response with the most efficient state

@app.route("/GetLeastEfficientState") # Defining a route to get the least efficient state in handling COVID cases
def getLeastEfficientState():
    ans = spark.sql("SELECT State, Cured/Total as ans FROM table ORDER BY ans LIMIT 1").collect()[0][0]
    return jsonify({
        'Least_efficienct_state': ans
    })


if(__name__ == '__main__'):
    app.run(host='0.0.0.0',port=8001)

