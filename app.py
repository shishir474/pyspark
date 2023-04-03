from flask import Flask, jsonify
from demo import df,spark

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({'/most_affected_state': "Most affected state among all the states ( total death/total covid cases)",
                    '/least_affected_state': "Least affected state among all the states ( total death/total covid cases)",
                    '/highest_covid_cases': "State with highest covid cases.",
                    '/least_covid_cases': "State with least covid cases.",
                    '/total_cases': "Total cases.",
                    '/most_efficient_state':"State that handled the covid most efficiently( total recovery/ total covid cases).",
                    '/least_efficient_state': "State that handled the covid least efficiently( total recovery/ total covid cases).",
                    '/show_all_data' : "Show all data"
                })


df.createOrReplaceTempView('table')

@app.route('/ShowMostAffectedState')
def getMostAffectedState():
    ans = spark.sql('SELECT State, Confirm/Death as ans FROM table ORDER BY ans DESC LIMIT 1').select('State').collect()[0][0]
    return jsonify({
        'most_affected_state': ans
    })


@app.route('/LeastAffectedState')
def getLeastAffectedState():
    ans = spark.sql('SELECT State, Confirm/Death as ans FROM table ORDER BY ans limit 1').select('State').collect()[0][0]
    return jsonify({
        'most_affected_state': ans
    })
    

@app.route('/StateWithMaxCovidCases')
def getStateWithMaxCovidCases():
    ans = spark.sql("Select State FROM table ORDER BY Confirm DESC LIMIT 1").collect()[0][0]
    return jsonify({
        'State_with_max_covid_cases': ans
    })


@app.route('/StateWithMinCovidCases')
def getStateWithMinCovidCases():
    ans = spark.sql("Select State FROM table ORDER BY Confirm LIMIT 1").collect()[0][0]
    return jsonify({
        'State_with_min_covid_cases': ans
    })

    
@app.route('/GetTotalCases')
def getTotalCases():
    ans = spark.sql("SELECT SUM(Confirm) as Total_cases FROM table").collect()[0][0]
    return jsonify({
        'Total_cases': ans
    })


@app.route("/GetMostEfficientState")
def getMostEfficientState():
    ans = spark.sql("SELECT State, Cured/Total as ans FROM table ORDER BY ans DESC LIMIT 1").collect()[0][0]
    return jsonify({
        'Most_efficienct_state': ans
    })


@app.route("/GetLeastEfficientState")
def getLeastEfficientState():
    ans = spark.sql("SELECT State, Cured/Total as ans FROM table ORDER BY ans LIMIT 1").collect()[0][0]
    return jsonify({
        'Least_efficienct_state': ans
    })


if(__name__ == '__main__'):
    app.run(host='0.0.0.0',port=8001)