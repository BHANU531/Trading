# streamlit/streamlit_app.py
import os
from datetime import datetime, timedelta
import duckdb
import plotly.graph_objs as go
import streamlit as st
import pandas as pd

# Update data directory to match Docker volume
DATA_DIR = "/app/data"

def get_qqq_tickers():
    df = pd.read_csv("nas100_ticker.txt")
    return df['Ticker'].tolist()

st.title("📈 Stock Data Viewer")



data_options = ['HISTORICAL DATA', 'MINUTE DATA', 'OPTIONS', 'DIVIDENDS']
data_option = st.selectbox("Select a data type:", data_options)

ticker_options = get_qqq_tickers()
ticker_option = st.selectbox("Select a Ticker:", ticker_options)

duckdb_conn = duckdb.connect()


@st.cache_data
def fetch_data(folder, ticker_option):
    file_path = os.path.join(DATA_DIR, folder, f"{ticker_option}.parquet")
    print(file_path)
    if os.path.exists(file_path):
        df = duckdb_conn.query(f"SELECT * FROM '{file_path}'").df()
        return df
    return None


def history_graph(folder, ticker_option):
    df = fetch_data(folder, ticker_option)
    if df is None:
        st.error(f"No data available for {ticker_option} ")
        return

    st.title(f"📈 {ticker_option} Stock Price Visualization")

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=df["Date"],
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
    ))
    fig.update_layout(
        title=f"{ticker_option} Stock Prices",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)

    fig_volume = go.Figure()
    fig_volume.add_trace(go.Bar(
        x=df["Date"],
        y=df["Volume"],
        name="Volume"
    ))
    fig_volume.update_layout(
        title=f"{ticker_option} Trading Volume",
        xaxis_title="Date",
        yaxis_title="Volume",
        height=300
    )
    st.plotly_chart(fig_volume, use_container_width=True)


def minute_graph(folder, ticker_option):
    df = fetch_data(folder, ticker_option)
    if df is None:
        st.error(f"No minute data available for {ticker_option} ")
        return

    st.title(f"📈 {ticker_option} Minute-Level Price Data")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", df["Datetime"].min())
    with col2:
        end_date = st.date_input("End Date", df["Datetime"].max())

    mask = (df["Datetime"].dt.date >= start_date) & (df["Datetime"].dt.date <= end_date)
    filtered_df = df[mask]

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=filtered_df["Datetime"],
        open=filtered_df["Open"],
        high=filtered_df["High"],
        low=filtered_df["Low"],
        close=filtered_df["Close"],
    ))
    fig.update_layout(
        title=f"{ticker_option} Minute-Level Prices",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)

    fig_volume = go.Figure()
    fig_volume.add_trace(go.Bar(
        x=filtered_df["Datetime"],
        y=filtered_df["Volume"],
        name="Volume"
    ))
    fig_volume.update_layout(
        title=f"{ticker_option} Minute-Level Volume",
        xaxis_title="Time",
        yaxis_title="Volume",
        height=300
    )
    st.plotly_chart(fig_volume, use_container_width=True)


def options_graph(folder, ticker_option):
    calls_folder = folder + "_calls"
    puts_folder = folder + "_puts"
    calls = fetch_data(calls_folder, ticker_option)
    puts = fetch_data(puts_folder, ticker_option)

    if calls is None or puts is None:
        st.error(f"No options data available for {ticker_option}")
        return

    min_strike = min(min(calls['strike']), min(puts['strike']))
    max_strike = max(max(calls['strike']), max(puts['strike']))

    strike_range = st.slider(
        "Select Strike Price Range",
        float(min_strike),
        float(max_strike),
        (float(min_strike), float(max_strike))
    )

    calls = calls[(calls['strike'] >= strike_range[0]) & (calls['strike'] <= strike_range[1])]
    puts = puts[(puts['strike'] >= strike_range[0]) & (puts['strike'] <= strike_range[1])]

    fig_calls = go.Figure()
    fig_calls.add_trace(
        go.Scatter(x=calls['strike'], y=calls['lastPrice'], mode='lines+markers', name='Calls'))
    fig_calls.update_layout(
        title=f'Calls for {ticker_option}',
        xaxis_title='Strike Price',
        yaxis_title='Last Price',
        height=400
    )

    fig_puts = go.Figure()
    fig_puts.add_trace(
        go.Scatter(x=puts['strike'], y=puts['lastPrice'], mode='lines+markers', name='Puts'))
    fig_puts.update_layout(
        title=f'Puts for {ticker_option}',
        xaxis_title='Strike Price',
        yaxis_title='Last Price',
        height=400
    )

    st.plotly_chart(fig_calls, use_container_width=True)
    st.plotly_chart(fig_puts, use_container_width=True)


def dividend_graph(folder, ticker_option):
    df = fetch_data(folder, ticker_option)
    if df is None:
        st.error(f"No dividend data available for {ticker_option} ")
        return

    st.title(f"💰 {ticker_option} Dividend History")

    total_dividends = df['Dividend'].sum()
    avg_dividend = df['Dividend'].mean()
    max_dividend = df['Dividend'].max()
    min_dividend = df['Dividend'].min()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Dividends", f"${total_dividends:.2f}")
    col2.metric("Average Dividend", f"${avg_dividend:.2f}")
    col3.metric("Highest Dividend", f"${max_dividend:.2f}")
    col4.metric("Lowest Dividend", f"${min_dividend:.2f}")

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df["Date"],
        y=df["Dividend"],
        name="Dividend Amount"
    ))

    df['12M_Avg'] = df['Dividend'].rolling(window=4).mean()
    fig.add_trace(go.Scatter(
        x=df["Date"],
        y=df["12M_Avg"],
        name="12-Month Average",
        line=dict(color='red')
    ))

    fig.update_layout(
        title=f"{ticker_option} Dividend History",
        xaxis_title="Date",
        yaxis_title="Dividend Amount (USD)",
        height=500,
        showlegend=True
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Dividend History Table")
    st.dataframe(
        df.sort_values('Date', ascending=False)
        .style.format({'Dividend': '${:.2f}'})
    )


# Main application logic
if data_option == 'HISTORICAL DATA':
    folder = 'historical_data'
    history_graph(folder, ticker_option)
elif data_option == 'MINUTE DATA':
    folder = 'minute_data'
    minute_graph(folder, ticker_option)
elif data_option == 'OPTIONS':
    folder = 'options_data'
    options_graph(folder, ticker_option)
else:  # DIVIDENDS
    folder = 'dividend_data'
    dividend_graph(folder, ticker_option)