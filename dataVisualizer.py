import dataAnalyzer

def filter_by_year(year):
    print(dataAnalyzer.filter_by_year(year))

def ticker_balance(selected_ticker):
    result, remaining = dataAnalyzer.ticker_balance(selected_ticker)
    print('Results of ' + selected_ticker + ': ' + str(result))
    if remaining > 0:
        print('Existing position: ' + str(remaining))
    return result

def total_balance():
    sum = 0
    for ticker in dataAnalyzer.total_balance():
        sum += ticker_balance(ticker)
    print("Total earnings: " + str(sum))