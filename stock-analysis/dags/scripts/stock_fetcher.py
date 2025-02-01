import asyncio
import os
import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as fin

# Update the data directory to match Docker volume
DATA_DIR = "/opt/airflow/data"


def get_qqq_tickers():
    df = pd.read_csv("dags/scripts/nas100_ticker.txt")
    return df['Ticker'].tolist()


async def fetch_data(ticker, fetch_type, session):
    """
    Generic async function to fetch different types of data
    """
    try:
        company_stock = fin.Ticker(ticker)

        if fetch_type == "minute":
            data = company_stock.history(period='7d', interval='1m')
            if not data.empty:
                data['company'] = ticker
                data = data.reset_index()
                return ("minute_data", data, ticker)

        elif fetch_type == "dividend":
            dividends = company_stock.dividends
            if not dividends.empty:
                dividends_df = dividends.reset_index()
                dividends_df.columns = ['Date', 'Dividend']
                dividends_df['company'] = ticker
                return ("dividend_data", dividends_df, ticker)

        elif fetch_type == "historical":
            history = company_stock.history(period='10y', interval='1d', actions=True)
            if not history.empty:
                history['company'] = ticker
                history = history.reset_index()
                return ("historical_data", history, ticker)

        elif fetch_type == "options":
            if not company_stock.options or len(company_stock.options) == 0:
                print(f"No options data available for {ticker}")
                return None

            try:
                recent_expire = company_stock.options[0]
                options_chain = company_stock.option_chain(recent_expire)

                if options_chain.calls.empty or options_chain.puts.empty:
                    print(f"Incomplete options data for {ticker}")
                    return None

                calls = pd.DataFrame(options_chain.calls)
                puts = pd.DataFrame(options_chain.puts)
                calls['company'] = ticker
                puts['company'] = ticker
                calls.reset_index(drop=True, inplace=True)
                puts.reset_index(drop=True, inplace=True)

                return [
                    ("options_data_calls", calls, ticker),
                    ("options_data_puts", puts, ticker)
                ]
            except Exception as options_error:
                print(f"Error processing options for {ticker}: {options_error}")
                return None

    except Exception as e:
        print(f"Error fetching {fetch_type} data for {ticker}: {e}")
        return None


def save_parquets(data_dir, data, ticker):
    """
    Save data to parquet file, checking for and replacing existing files
    """
    try:
        # Create data directory if it doesn't exist
        full_dir = os.path.join(DATA_DIR, data_dir)
        if not os.path.exists(full_dir):
            os.makedirs(full_dir)

        if isinstance(data, pd.DataFrame) and not data.empty:
            parquet_file = os.path.join(full_dir, f"{ticker}.parquet")

            # Check if file exists and remove it
            if os.path.exists(parquet_file):
                try:
                    os.remove(parquet_file)
                    print(f"Removed existing parquet file for {ticker}")
                except Exception as e:
                    print(f"Error removing existing parquet file for {ticker}: {e}")
                    return

            # Save new parquet file
            table = pa.Table.from_pandas(data)
            pq.write_table(table, parquet_file)
            print(f"Successfully saved new parquet file for {ticker}")
        else:
            print(f"No data to save for {ticker} in {data_dir}")
    except Exception as e:
        print(f"Error saving parquet for {ticker} in {data_dir}: {e}")


async def process_ticker(ticker, session):
    tasks = []
    for data_type in ["minute", "dividend", "historical", "options"]:
        task = fetch_data(ticker, data_type, session)
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if result is None or isinstance(result, Exception):
            continue

        try:
            if isinstance(result, list):  # Handle options data which returns two datasets
                for data_dir, data, ticker_suffix in result:
                    save_parquets(data_dir, data, ticker_suffix)
            else:
                data_dir, data, ticker_suffix = result
                save_parquets(data_dir, data, ticker_suffix)
        except Exception as e:
            print(f"Error processing result for {ticker}: {e}")


async def main():
    """
    Main async function to orchestrate the data collection
    """
    tickers = get_qqq_tickers()

    connector = aiohttp.TCPConnector(limit_per_host=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for ticker in tickers:
            task = process_ticker(ticker, session)
            tasks.append(task)

        total_tasks = len(tasks)
        completed = 0

        for task in asyncio.as_completed(tasks):
            try:
                await task
                completed += 1
                print(f"Progress: {completed}/{total_tasks} tickers processed")
            except Exception as e:
                print(f"Error processing task: {e}")
                completed += 1


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()