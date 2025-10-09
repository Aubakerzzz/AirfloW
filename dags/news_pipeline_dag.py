from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os, string, nltk
from nltk.corpus import stopwords
from textblob import TextBlob

# Download NLTK data
nltk.download('stopwords')
nltk.download('wordnet')

# Correct path to the JSON dataset
DATA_PATH = "/opt/airflow/data/News_Category_Dataset_v3.json"

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def ingest_data():
    # Download and unzip the Kaggle dataset
    os.system("kaggle datasets download -d rmisra/news-category-dataset -p /opt/airflow/data --unzip")
    print("Dataset downloaded and extracted successfully!")

def basic_cleaning():
    # Read JSON directly
    df = pd.read_json(DATA_PATH, lines=True)
    
    # Ensure 'headline' column exists
    if 'headline' not in df.columns:
        raise ValueError("The dataset does not contain a 'headline' column.")
    
    # Clean text: lowercase and remove punctuation
    df['headline'] = df['headline'].str.lower().str.replace(f"[{string.punctuation}]", "", regex=True)
    
    # Save cleaned CSV for downstream steps
    df.to_csv("/opt/airflow/data/cleaned.csv", index=False)
    print("Basic cleaning completed and saved as cleaned.csv")

def advanced_processing():
    df = pd.read_csv("/opt/airflow/data/cleaned.csv")
    stop_words = set(stopwords.words("english"))
    
    # Remove stopwords safely
    df['headline'] = df['headline'].apply(lambda x: " ".join([w for w in str(x).split() if w not in stop_words]))
    
    df.to_csv("/opt/airflow/data/processed.csv", index=False)
    print("Advanced processing completed and saved as processed.csv")

def feature_engineering():
    df = pd.read_csv("/opt/airflow/data/processed.csv")
    
    # Fill missing headlines
    df['headline'] = df['headline'].fillna("")
    
    # Add text length feature
    df['text_length'] = df['headline'].apply(len)
    
    # Add sentiment feature
    df['sentiment'] = df['headline'].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
    
    df.to_csv("/opt/airflow/data/featured.csv", index=False)
    print("Feature engineering completed and saved as featured.csv")


def split_data():
    from sklearn.model_selection import train_test_split
    df = pd.read_csv("/opt/airflow/data/featured.csv")
    
    # Split dataset into train, validation, and test sets
    train, temp = train_test_split(df, test_size=0.3, random_state=42)
    val, test = train_test_split(temp, test_size=0.5, random_state=42)
    
    train.to_csv("/opt/airflow/data/train.csv", index=False)
    val.to_csv("/opt/airflow/data/val.csv", index=False)
    test.to_csv("/opt/airflow/data/test.csv", index=False)
    print("Data split into train, val, and test sets successfully")

with DAG(
    dag_id='news_data_pipeline',
    default_args=default_args,
    description='News Category Data Pipeline with Airflow',
    start_date=datetime(2025, 10, 9),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    ingest = PythonOperator(task_id='ingest_data', python_callable=ingest_data)
    clean = PythonOperator(task_id='basic_cleaning', python_callable=basic_cleaning)
    process = PythonOperator(task_id='advanced_processing', python_callable=advanced_processing)
    feature = PythonOperator(task_id='feature_engineering', python_callable=feature_engineering)
    split = PythonOperator(task_id='split_data', python_callable=split_data)

    ingest >> clean >> process >> feature >> split
