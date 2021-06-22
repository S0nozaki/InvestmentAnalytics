import json
import dataAnalyzer
from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/filter_by_year/<int:year>')
def filter_by_year(year):
    res = dataAnalyzer.filter_by_year(year).to_json(orient='records')
    return jsonify(json.loads(res))


def ticker_balance(selected_ticker):
    result, remaining = dataAnalyzer.get_ticker_balance(selected_ticker)
    for entry in result.keys():
        result[entry] = str(result[entry])
    return [result, remaining]


@app.route('/balance/<string:selected_ticker>')
def balance(selected_ticker):
    return jsonify(ticker_balance(selected_ticker))


@app.route('/total_balance')
def total_balance():
    tickers, currencies_counter = dataAnalyzer.get_total_metadata()
    result = {}
    for ticker in tickers:
        result[ticker] = ticker_balance(ticker)
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True, port=4000)
