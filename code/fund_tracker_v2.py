# Author: Jack Friedman
# Date: 2/1/2024
# Purpose: Program scrapes 13F SEC filing data about hedge fund holdings from 13f.info and displays portfolios over time in a streamlit dashboard
# To run code: Navigate to file directory in terminal and execute "streamlit run fund_tracker.py"

from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import streamlit as st
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')

# Controls the maximum number of holdings to display in the graph
MAX_HOLDINGS = 300

# Scrapes the list of all hedge funds on 13f.info
@st.cache_data
def gather_fund_list():
    firm_list_df = pd.DataFrame(columns=['Name', 'URL'])

    entries = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0']
    for key in entries:
        # Make request
        url = "https://13f.info/managers/" + key

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        # Extract table of firms
        table = soup.find('table') 

        # Extract hyperlinks
        data = []
        for row in table.find_all('tr'):
            cells = row.find_all('td')
            if cells:
                hyperlink = cells[0].find('a')
                if hyperlink:
                    text = hyperlink.get_text()  # The text of the hyperlink
                    link = hyperlink['href']  # The URL of the hyperlink
                    link = "https://13f.info" + link
                    data.append([text, link])

        # Add to dataframe
        df = pd.DataFrame(data, columns=['Name', 'URL'])
        firm_list_df = pd.concat([firm_list_df, df])

    # firm_list_df.to_csv("firm_list.csv")
    return firm_list_df

# Scrapes the filing info for a given hedge fund
@st.cache_data
def scrape_filings(homepage_url  = "https://13f.info/manager/0001697868-valley-forge-capital-management-lp"):
    # Scrape the page and extract the table of filings for that firm
    page = requests.get(homepage_url)
    index_table = pd.read_html(page.content, converters={"Filing ID": str})[0]

    df = pd.DataFrame(columns=['Quarter', 'Date Filed', 'Ticker', 'Company Name', 'Class', 'CUSIP', 'Value ($000)', 'Percentage', 'Shares', 'Principal', 'Option Type'])
    for filing_id in list(index_table['Filing ID']):
        url = 'https://13f.info/data/13f/' + str(filing_id)

        # Send a GET request to the website
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')

        # Get JSON object of data
        data = json.loads(str(soup))

        # Put into data frame
        filing_df = pd.DataFrame(data['data'], columns=['Ticker', 'Company Name', 'Class', 'CUSIP', 'Value ($000)', 'Percentage', 'Shares', 'Principal', 'Option Type'])

        filing_df['Quarter'] = index_table[index_table["Filing ID"] == filing_id]['Quarter'].iloc[0]
        filing_df['Date Filed'] = index_table[index_table["Filing ID"] == filing_id]['Date Filed'].iloc[0]

        df = pd.concat([df, filing_df])

    # df.to_csv("all_holdings_data.csv")
    return df

@st.cache_data
def preprocess_filings_df(df):
    # Convert 'Date Filed' to datetime
    df['Date Filed'] = pd.to_datetime(df['Date Filed'])

    # Adjust 'Value' by multiplying by 1000
    df['Value ($000)'] = 1000 * df['Value ($000)']
    df = df.rename({'Value ($000)': 'Value'}, axis=1)  # Rename column for clarity

    # Drop rows with NaN in 'Percentage'
    df = df.dropna(subset=['Percentage'])
    
    # Create new variable for quarter end date (to use instead of filing date since these are uniformly spaced)
    def get_quarter_end(quarter_str):
        year, quarter = int(quarter_str.split(' ')[1]), quarter_str.split(' ')[0]
        quarter_end_dates = {
            'Q1': datetime(year, 3, 31),
            'Q2': datetime(year, 6, 30),
            'Q3': datetime(year, 9, 30),
            'Q4': datetime(year, 12, 31),
        }
        return quarter_end_dates[quarter]
    
    df['quarter_end'] = df['Quarter'].apply(get_quarter_end)
    df['quarter_end'] = pd.to_datetime(df['quarter_end']) # Ensure 'quarter_end' is datetime
    
    # Sort chronologically
    df.sort_values(by='quarter_end', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Rename Ticker if it's a put/call option
    df['ticker_adjusted'] = df['Ticker']
    df.loc[df['Option Type'] == 'put', 'ticker_adjusted'] += ' (put)'
    df.loc[df['Option Type'] == 'call', 'ticker_adjusted'] += ' (call)'

    # Get a list of all unique quarter end dates in the DataFrame
    all_quarters = sorted(df['quarter_end'].unique())

    # Add rows with 0 shares in between the time a firm sells and then repurchases the same holding
    updated_rows = []
    for ticker in df['ticker_adjusted'].unique():  
        # Filter the DataFrame for just that ticker
        ticker_df = df[df['ticker_adjusted'] == ticker]
        if len(ticker_df) == 0:
            continue

        # Get min and max quarter end dates for that ticker
        min_date, max_date = ticker_df['quarter_end'].min(), ticker_df['quarter_end'].max()

        # Add new row with any missing quarter end dates as 0
        for quarter in all_quarters:
            if quarter < min_date or quarter > max_date:
                continue  # Skip quarters outside the holding period for this ticker
            if quarter not in ticker_df['quarter_end'].values:
                # If the quarter is missing, add a new row with Value/Number Shares/Percentage as 0
                updated_rows.append({'Quarter': df[df['quarter_end'] == quarter]['Quarter'].iloc[0],
                                      'Date Filed':df[df['quarter_end'] == quarter]['Date Filed'].iloc[0], 
                                      'Ticker':ticker, 
                                      'Company Name':ticker_df['Company Name'].iloc[0], 
                                      'Class': ticker_df['Class'].iloc[0], 
                                      'CUSIP': ticker_df['CUSIP'].iloc[0], 
                                      'Value': 0, 
                                      'Percentage': 0, 
                                      'Shares': 0, 
                                      'Principal':ticker_df['Principal'].iloc[0], 
                                      'Option Type': ticker_df['Option Type'].iloc[0],
                                    'quarter_end': quarter})
            else:
                # Add existing rows
                updated_rows.append(ticker_df[ticker_df['quarter_end'] == quarter].iloc[0].to_dict())

    updated_df = pd.DataFrame(updated_rows)

    # Sort the DataFrame 
    updated_df = updated_df.sort_values(by=['Ticker', 'quarter_end'])
    updated_df = updated_df.reset_index(drop=True)
    return updated_df

def filter_date_range(df, date_range):
    # Select the appropriate start and end date based on user unput
    end_date = df['quarter_end'].max()
    if date_range == '1Y':
        start_date = max(df['quarter_end'].min(), df['quarter_end'].max() - timedelta(days=365))
    elif date_range == '3Y':
        start_date = max(df['quarter_end'].min(), df['quarter_end'].max() - timedelta(days=3*365))
    elif date_range == '5Y':
        start_date = max(df['quarter_end'].min(), df['quarter_end'].max() - timedelta(days=5*365))
    elif date_range == 'Max':
        start_date = df['quarter_end'].min()
    else:
        start_date = st.date_input("Start date", value=df['quarter_end'].min())
        end_date = st.date_input("End date", value=df['quarter_end'].max())

    # Filter date range based on input
    df = df[(df['quarter_end'] >= pd.to_datetime(start_date)) & (df['quarter_end'] <= pd.to_datetime(end_date))]
    
    return df

@st.cache_data
def filter_top_k_holdings(df_filtered, k):
    # Get a list of all the top holdings at the end date
    df_filtered = df_filtered.sort_values(by='quarter_end', ascending=True)
    end_date = df_filtered['quarter_end'].max()
    df_end_date = df_filtered[df_filtered['quarter_end'] == end_date]
    df_end_date_sorted = df_end_date.sort_values(by='Percentage', ascending=False)

    # Filter top k holdings
    top_holdings = df_end_date_sorted['Ticker'].head(k).unique()
    df_filtered = df_filtered[df_filtered['Ticker'].isin(top_holdings)]
    
    return df_filtered


@st.cache_data
def make_graph(df_filtered, y_axis):
    y_axis_mapping = {
        "Percentage of portfolio": "Percentage",
        "Number of shares": "Shares",
        "Value": "Value"
    }

    # Adjust ticker label for put/call options
    df_filtered['ticker_adjusted'] = df_filtered['Ticker']
    df_filtered.loc[df_filtered['Option Type'] == 'put', 'ticker_adjusted'] += ' (put)' # For rows where 'Option Type' is 'put', append '_put' to the 'color' value
    df_filtered.loc[df_filtered['Option Type'] == 'call', 'ticker_adjusted'] += ' (call)' # For rows where 'Option Type' is 'put', append '_put' to the 'color' value

    # Adjust title if there are options in the portfolio
    if df_filtered['ticker_adjusted'].str.contains('(call)').any() or df_filtered['ticker_adjusted'].str.contains('(put)').any():
        title=f"{y_axis} over time<br><sub>(Options displayed with dashed lines)</sub>"
    else:
        title=f"{y_axis} over time"

    # Graph plot 
    fig = px.line(df_filtered, x='quarter_end', y=y_axis_mapping[y_axis],
                    labels={
                        "quarter_end": "Quarter",
                        "ticker_adjusted": "Ticker"},
                    color='ticker_adjusted', markers=True,
                    title = title)
    
    # Update the x acis labels
    fig.update_xaxes(
        tickvals=df_filtered['quarter_end'],
        ticktext=df_filtered['Quarter']
    )

    # Add dashed lines for put/call options
    fig.update_traces(mode='markers+lines')
    for trace in fig.data:
        if '(put)' in trace.name or '(call)' in trace.name:  # Assuming that 'put' has been appended to the name in the color column
            trace.line.dash = 'dash'  # Set the line to be dashed

    fig.update_layout(legend_title_text='Ticker')
    st.plotly_chart(fig)
    st.markdown(f"Data source: [13f.info]({selected_url})")  # display data source
    


# Streamlit UI
st.title('Hedge Fund Portfolio Tracker')
st.subheader("View the holdings of over 10,000 hedge funds")

# Scrape fund list
firm_list_df = gather_fund_list()

# User input: select fund
options = [""] + firm_list_df['Name'].tolist()
selected_option = st.selectbox("Select fund", options)

# Check if a company has been selected (i.e., selected option is not empty)
if selected_option:

    # When a name is selected, get the corresponding URL
    selected_url = firm_list_df.loc[firm_list_df['Name'] == selected_option, 'URL'].iloc[0]

    # Scrape that firm's holdings
    df = scrape_filings(selected_url)

    # Preprocess data
    df = preprocess_filings_df(df)

    # Get user input: date range
    date_range = st.selectbox('Select date range', ('1Y', '3Y', '5Y', 'Max', 'Custom'))

    # Filter date range
    df_filtered = filter_date_range(df, date_range)

    # Get user input: y-axis value
    y_axis = st.selectbox('Select Y-axis value', ('Percentage of portfolio', 'Number of shares', 'Value'))

    # Get user input: number of holdings to display
    holdings_filter_option = st.selectbox('Filter by number of holdings', ['All holdings', 'Top 5 holdings', 'Top 10 holdings'])
    
    selected_additional_tickers = []

    # Filter the DataFrame based on the selected holdings option
    if holdings_filter_option == 'Top 5 holdings':
        df_filtered = filter_top_k_holdings(df_filtered, 5)
    elif holdings_filter_option == 'Top 10 holdings':
        df_filtered = filter_top_k_holdings(df_filtered, 10)
    # Check if more than MAX_HOLDINGS
    elif holdings_filter_option == 'All holdings' and len(df_filtered['ticker_adjusted'].unique()) > MAX_HOLDINGS:
        # Only display the top MAX_HOLDINGS and let user select additional tickers
        top_holdings_df = filter_top_k_holdings(df_filtered, MAX_HOLDINGS)

        prompt = "Select additional tickers (optional, only displaying the top " + str(MAX_HOLDINGS) + " out of " + str(len(df_filtered['ticker_adjusted'].unique())) + " total holdings)"
        selected_additional_tickers = st.multiselect(prompt, df_filtered['ticker_adjusted'].unique())
        additional_tickers_df = df_filtered[df_filtered['ticker_adjusted'].isin(selected_additional_tickers)]
        
        df_filtered = pd.concat([top_holdings_df, additional_tickers_df])
    
    # Make the graph
    make_graph(df_filtered, y_axis)

# add signature
st.markdown("Created by Jack Friedman ([LinkedIn](https://www.linkedin.com/in/jack-friedman/), [Blog](https://jackfriedman.substack.com/))")
