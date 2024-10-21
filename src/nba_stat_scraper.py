#!/usr/bin/env python
# coding: utf-8

"""
NBA Player Statistics Scraper and Database Loader

This script scrapes NBA player per-game statistics from Basketball Reference,
cleans and processes the data, and saves it to a MySQL database.

Requirements:
- Python 3.x
- requests
- beautifulsoup4
- pandas
- sqlalchemy
- pymysql

Usage:
- Install the required packages: pip install -r requirements.txt
- Set up your MySQL database and update the connection parameters if needed.
- Run the script: python nba_stats_scraper.py

Note:
- Sensitive information like database passwords should be handled securely.
- Use environment variables or a configuration file to store credentials.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_nba_data(url):
    """
    Scrapes NBA player per-game statistics from the given URL.

    Parameters:
    - url (str): URL of the NBA stats page.

    Returns:
    - df (pd.DataFrame): DataFrame containing the scraped data.
    """
    logger.info(f"Fetching data from {url}")
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"Failed to retrieve data from {url}")
        return None
    page_source = response.text
    soup = BeautifulSoup(page_source, 'html.parser')

    # Extract the table header
    nba_table_header = soup.find('tr')
    header_list = nba_table_header.find_all('th')
    headers = [col.text for col in header_list]

    # Extract the table body
    nba_body = soup.find('tbody').find_all('tr')

    data = []
    for row in nba_body:
        first_col = row.find('th')
        if first_col:  # Ensure that first_col is not None
            first_col_text = first_col.text
        else:
            first_col_text = ''
        other_cols = [col.text for col in row.find_all('td')]
        row_data = [first_col_text] + other_cols
        data.append(row_data)

    df = pd.DataFrame(data, columns=headers)
    logger.info("Data fetched successfully")
    return df

def clean_data(df):
    """
    Cleans and processes the NBA data DataFrame.

    Parameters:
    - df (pd.DataFrame): Raw DataFrame to be cleaned.

    Returns:
    - df (pd.DataFrame): Cleaned DataFrame.
    """
    logger.info("Cleaning data")

    # Remove any leading/trailing whitespace from player names
    df['Player'] = df['Player'].str.strip()

    # Remove duplicate header rows within the data
    df = df[df['Rk'] != 'Rk']

    # List of columns to convert to numeric
    numeric_columns = [
        'G', 'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
        '2P', '2PA', '2P%', 'eFG%', 'FT', 'FTA', 'FT%', 'ORB',
        'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'
    ]

    # Convert columns to numeric types
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Fill NaN values in numeric columns with zeros
    df[numeric_columns] = df[numeric_columns].fillna(0)

    # Rename columns to more descriptive names
    df.rename(columns={
        'Rk': 'Rank',
        'Player': 'Player_Name',
        'Age': 'Player_Age',
        'Tm': 'Team_Name',
        'Pos': 'Position',
        'G': 'Games_Played',
        'GS': 'Games_Started',
        'MP': 'Minutes_Played',
        'FG': 'Field_Goals_Made',
        'FGA': 'Field_Goals_Attempted',
        'FG%': 'Field_Goal_Percentage',
        '3P': 'Three_Pointers_Made',
        '3PA': 'Three_Pointers_Attempted',
        '3P%': 'Three_Point_Percentage',
        '2P': 'Two_Pointers_Made',
        '2PA': 'Two_Pointers_Attempted',
        '2P%': 'Two_Point_Percentage',
        'eFG%': 'Effective_Field_Goal_Percentage',
        'FT': 'Free_Throws_Made',
        'FTA': 'Free_Throws_Attempted',
        'FT%': 'Free_Throw_Percentage',
        'ORB': 'Offensive_Rebounds',
        'DRB': 'Defensive_Rebounds',
        'TRB': 'Total_Rebounds',
        'AST': 'Assists',
        'STL': 'Steals',
        'BLK': 'Blocks',
        'TOV': 'Turnovers',
        'PF': 'Personal_Fouls',
        'PTS': 'Points_Per_Game',
    }, inplace=True)

    logger.info("Data cleaned successfully")
    return df

def connect_to_database():
    """
    Creates a SQLAlchemy engine for connecting to the MySQL database.

    Returns:
    - engine: SQLAlchemy engine object.
    """
    logger.info("Connecting to the database")
    # Retrieve database credentials from environment variables
    user = os.getenv('DB_USER', 'root')
    password = os.getenv('DB_PASSWORD', 'your_password')
    host = os.getenv('DB_HOST', 'localhost')
    database = os.getenv('DB_NAME', 'nba_stats')
    port = os.getenv('DB_PORT', '3306')

    # Encode password in case it contains special characters
    encoded_password = urllib.parse.quote_plus(password)

    # Create database engine
    engine_url = f'mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}'
    try:
        engine = create_engine(engine_url)
        # Test the connection
        with engine.connect() as conn:
            logger.info("Database connection successful")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return None

def save_to_database(df, engine, table_name='player_stats'):
    """
    Saves the DataFrame to the specified table in the database.

    Parameters:
    - df (pd.DataFrame): DataFrame to be saved.
    - engine: SQLAlchemy engine object.
    - table_name (str): Name of the database table.
    """
    logger.info(f"Saving data to the database table '{table_name}'")
    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logger.info("Data saved to database successfully")
    except Exception as e:
        logger.error(f"An error occurred while saving to the database: {e}")

def main():
    # URL of the NBA stats page
    nba_stats_url = "https://www.basketball-reference.com/leagues/NBA_2024_per_game.html"

    # Fetch the data
    df = get_nba_data(nba_stats_url)
    if df is None:
        logger.error("Data retrieval failed. Exiting.")
        return

    # Clean the data
    cleaned_df = clean_data(df)

    # Connect to the database
    engine = connect_to_database()
    if engine is None:
        logger.error("Database connection failed. Exiting.")
        return

    # Save the data to the database
    save_to_database(cleaned_df, engine)

if __name__ == "__main__":
    main()
