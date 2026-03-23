import yfinance as yf
import pandas as pd
from datetime import datetime

TICKERS = [
    "SPY", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "TSLA", "JPM", "BAC", "WMT", "COST", "AMD"
]

START_DATE = "2014-01-01"
END_DATE = datetime.today().strftime('%Y-%m-%d')


def fetch_data(ticker):
    df = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)
    df = df.reset_index()

    df["ticker"] = ticker

    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })

    return df[["date", "ticker", "open", "high", "low", "close", "volume"]]


def main():
    all_data = []

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        df = fetch_data(ticker)
        all_data.append(df)

    final_df = pd.concat(all_data, ignore_index=True)

    final_df = final_df.sort_values(by=["ticker", "date"])

    output_path = "data/raw/prices.parquet"
    final_df.to_parquet(output_path, index=False)

    print(f"\nSaved data to {output_path}")
    print(final_df.head())


if __name__ == "__main__":
    main()