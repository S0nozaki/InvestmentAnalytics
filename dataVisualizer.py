import dataAnalyzer


def filter_by_year(year):
    print(dataAnalyzer.filter_by_year(year))


def ticker_balance(selected_ticker):
    result, remaining = dataAnalyzer.get_ticker_balance(selected_ticker)
    for entry in result.keys():
        print('Results of ' + selected_ticker +
              ' in ' + entry + ": " + str(result[entry]))
    if remaining > 0:
        print('Existing position: ' + str(remaining))
    return result


def total_balance():
    tickers, currencies_counter = dataAnalyzer.get_total_metadata()
    for ticker in tickers:
        result = ticker_balance(ticker)
        for currency_entry in result.keys():
            currencies_counter[currency_entry] += result[currency_entry]
    for currency in currencies_counter.keys():
        print("Total earnings in " + currency +
              ": " + str(currencies_counter[currency]))
