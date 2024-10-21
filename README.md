# NBA Player Stats ETL Project

## Table of Contents

- [Introduction](#introduction)
- [Project Description](#project-description)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Data Source](#data-source)


## Introduction

This project is an ETL (Extract, Transform, Load) pipeline that scrapes NBA player per-game statistics from Basketball Reference, cleans and processes the data, and loads it into a MySQL database. The goal is to provide a structured dataset for analysis and visualization of NBA player performance.

## Project Description

The NBA Player Stats ETL Project automates the process of collecting NBA player statistics for the 2023-2024 season. It extracts data from the [Basketball Reference](https://www.basketball-reference.com/leagues/NBA_2024_per_game.html) website, transforms the data by cleaning and normalizing it, and loads it into a MySQL database for further analysis.

The ETL process includes:

- **Extraction**: Scraping the per-game statistics table from the website.
- **Transformation**: Cleaning the data by handling encoding issues, converting data types, and renaming columns for clarity.
- **Loading**: Connecting to a MySQL database and inserting the cleaned data into a table.

This project demonstrates skills in web scraping, data cleaning, database operations, and Python programming.


## Features

- **Web Scraping**: Uses BeautifulSoup to scrape data from HTML tables, including those hidden within comments.
- **Data Cleaning**: Handles encoding issues, converts data types, and fills missing values.
- **Database Integration**: Connects to a MySQL database using SQLAlchemy and PyMySQL.
- **Modular Code**: Organized into functions for better readability and maintainability.
- **Logging**: Implements logging to track the ETL process and errors.

## Technologies Used

- **Programming Language**: Python 3.x
- **Web Scraping**: `requests`, `beautifulsoup4`
- **Data Manipulation**: `pandas`
- **Database Interaction**: `SQLAlchemy`, `PyMySQL`
- **Logging**: `logging` module
- **Database**: MySQL

## Prerequisites

- **Python 3.x** installed on your system.
- **MySQL Server** installed and running.
- **MySQL Database** named `nba_stats` (or your preferred name).
- **Python Packages** listed in `requirements.txt`.


## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/patrhick1/nba-player-stats-etl.git
   cd nba-player-stats-etl


   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   
   pip install -r requirements.txt
   
   Set up environment variables for Database
   
   python nba_stats_scraper.py



## **Usage**

Instructions on how to use the project after installation.

After running the script, the `player_stats` table in your MySQL database will be populated with the latest NBA player statistics. You can then use SQL queries or a visualization tool to analyze the data.
   
Example SQL query:
   
   ```sql
   SELECT * FROM player_stats WHERE Points_Per_Game > 20 ORDER BY Points_Per_Game DESC;



#### **Configuration**
   
Detailed instructions on setting environment variables.



To keep your database credentials secure, the script uses environment variables. Here's how you can set them up:
   
### **On Windows**
   
1. Open the **System Properties** dialog (`Win + Pause/Break`).
2. Click on **Advanced system settings**.
3. Click on **Environment Variables**.
4. Under **User variables**, click **New** and add the following:

- **Variable name**: `DB_USER`
- **Variable value**: Your MySQL username (e.g., `root`)

Repeat this for `DB_PASSWORD`, `DB_HOST`, `DB_NAME`, and `DB_PORT`.

### **On macOS and Linux**
   
Add the following lines to your `~/.bash_profile`, `~/.bashrc`, or `~/.zshrc` file:
   
   ```bash
   export DB_USER='your_username'
   export DB_PASSWORD='your_password'
   export DB_HOST='localhost'
   export DB_NAME='nba_stats'
   export DB_PORT='3306'
   

## Data Source

Data is sourced from [Basketball Reference](https://www.basketball-reference.com/leagues/NBA_2024_per_game.html). Please review their terms of service and usage policies before using the data.

**Disclaimer:** This project is for educational purposes only.


