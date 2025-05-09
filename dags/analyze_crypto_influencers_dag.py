from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from mock.mock_fetch_tweets import mock_fetch_tweets
from mock.mock_fetch_price import mock_fetch_price
import re
import pandas as pd

# Import your actual logic from your app code (e.g. from common/api/twitter import fetch_tweets)
# Here, using placeholders for now
def fetch_recent_tweets():

    # Read influencer seed file
    df = pd.read_parquet("dags/data/influencer_seed.parquet", engine="pyarrow")

    all_tweets = []

    for _, row in df.iterrows():
        print("______________: ", _)
        print("Row: ", row)
        handle = row["username"]  # adjust if your column is named differently
        posts = mock_fetch_tweets(handle, count = 50)
        all_tweets.extend(posts)

    posts_df = pd.DataFrame(all_tweets)

    # Upload recent_tweets to /data for access by next task
    posts_df.to_parquet("dags/data/recent_tweets.parquet", engine="pyarrow", index=False)

def extract_promotions_from_tweets():
    
    # Read recent_tweets file
    df = pd.read_parquet("dags/data/recent_tweets.parquet", engine="pyarrow")

    # Filter out all tweets that do not contain a ticker symbol
    df_with_tickers = df[df['text'].str.contains(r'\$', na=False)]

    # Find tickers using regular expressions
    df_with_tickers['ticker'] = df_with_tickers['text'].apply(lambda x: re.findall(r'\$([A-Za-z0-9]+)', x))

    # Explode the tickers column to process each ticker seperately
    df_with_tickers_exploded = df_with_tickers.explode('ticker')

    # Sort by username and timestamp (there should be a created_at for every post)
    df_with_tickers_exploded['created_at'] = pd.to_datetime(df_with_tickers_exploded['created_at'])
    df_with_tickers_exploded.sort_values(by=['username', 'created_at'], ascending=[True, True], inplace=True)

    # Find the first time a crypto ticker was mentioned
    first_mentions = df_with_tickers_exploded.drop_duplicates(subset=['username', 'ticker'], keep='first')

    # Select relevant columns
    first_mentions = first_mentions[['username', 'ticker', 'created_at']]

    # Store first mentions in parquet format for next task group
    first_mentions.to_parquet("dags/data/first_mentions.parquet", engine="pyarrow", index=False)

def fetch_price_data():
    
    # Read first mentions to get tickers and dates for calculations
    df = pd.read_parquet("dags/data/first_mentions.parquet", engine="pyarrow")

    results = []

    for _, row in df.iterrows():
        past_price, current_price = mock_fetch_price(row['ticker'], row['created_at']) # Fetch Mock Profit/Loss
        pl_dollar = current_price - past_price
        pl_percent = 0
        
        # We divide by past_price regardless, so check to make sure is NOT zero else calculate percentage gained
        if (past_price == 0):
            pl_percent = None
        else:
            pl_percent = round(((current_price - past_price) / past_price) * 100, 2)

        results.append({
            'username': row['username'],
            'ticker': row['ticker'],
            'past_price': past_price,
            'current_price': current_price,
            'pl_dollar': pl_dollar,
            'pl_percent': pl_percent,
        })

        df_pl = pd.DataFrame(results)
        df_pl.to_parquet("dags/data/price_results.parquet")

def calculate_influencer_pl():

    # Read first mentions to get tickers and dates for calculations
    df = pd.read_parquet("dags/data/price_results.parquet", engine="pyarrow")

    grouped = df.groupby("username").agg(
        avg_pl_percent=('pl_percent', 'mean'),  # Calculate the mean of the profit/loss percentage
        num_promoted=('ticker', 'count')        # Count the number of unique coins promoted
    ).reset_index()

    print(grouped.head())

    grouped.to_parquet("dags/data/influencer_pl.parquet", engine="pyarrow")


def store_final_results():

    df = pd.read_parquet("dags/data/influencer_pl.parquet", engine="pyarrow")

    print("DF Head: " ,df.head())
    print("DF Columns: ", df.columns)
    print("DF: ", df)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='analyze_crypto_influencers',
    default_args=default_args,
    description='Analyze crypto influencers and calculate P/L of promoted tokens',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'influencers', 'pl-analysis']
) as dag:

    # Task 2: Fetch Tweets
    fetch_tweets = PythonOperator(
        task_id='fetch_recent_tweets',
        python_callable=fetch_recent_tweets
    )

    # Task 3: Extract Promotions
    extract_promotions = PythonOperator(
        task_id='extract_promotions_from_tweets',
        python_callable=extract_promotions_from_tweets
    )

    # Task Group: Price Handling
    with TaskGroup("price_tasks", tooltip="Handle fetching and storing crypto prices") as price_tasks:
        get_price_data = PythonOperator(
            task_id='fetch_price_data',
            python_callable=fetch_price_data
        )

    # Task 4: Calculate P/L
    calc_pl = PythonOperator(
        task_id='calculate_influencer_pl',
        python_callable=calculate_influencer_pl
    )

    # Task 5: Store final result
    store_results = PythonOperator(
        task_id='store_final_results',
        python_callable=store_final_results
    )

    # DAG Task Flow
    fetch_tweets >> extract_promotions >> price_tasks >> calc_pl >> store_results