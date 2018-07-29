import datetime

from technical.history import load_data


def test_load_ticker():
    days = datetime.datetime.today() - datetime.timedelta(days=10)

    ticker = load_data("BTC/USD", "1d", from_date=days.timestamp(), ccxt_api="bitmex", force=True)
    print(len(ticker))
    assert len(ticker) == 10
    days = datetime.datetime.today() - datetime.timedelta(days=100)

    ticker = load_data("BTC/USD", "1d", from_date=days.timestamp(), ccxt_api="bitmex", force=True)
    print(len(ticker))
    assert len(ticker) == 100
    days = datetime.datetime.today() - datetime.timedelta(days=10)

    ticker = load_data("BTC/USD", "4h", from_date=days.timestamp(), ccxt_api="bitmex", force=False)
    print(len(ticker))
    assert len(ticker) == 60
