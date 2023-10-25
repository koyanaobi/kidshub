#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pymongo import MongoClient
import pandas as pd
from pandas import json_normalize
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from pytz import timezone
from concurrent.futures import ThreadPoolExecutor


# In[3]:


def scheduled_function():
    # Getting data from MongoDB
    client = MongoClient("mongodb+srv://yash:obi@cluster.l2wecvt.mongodb.net/?retryWrites=true&w=majority")
    db = client["test"]
    collection = db["ratings"]
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    
    # Adding missing data
    date_str = '2023-10-15 09:15:30.123'
    date = pd.to_datetime(date_str)
    df.loc[0, 'time_stamp'] = date
    
    date_str = '2023-09-19 06:57:48.765'
    date = pd.to_datetime(date_str)
    df.loc[1, 'time_stamp'] = date_str
    
    date_str = '2023-10-13 08:42:12.539'
    date = pd.to_datetime(date_str)
    df.loc[2, 'time_stamp'] = date_str
    
    # Sectioning the ratings class-wise
    # Define the rating ranges and labels
    rating_bins = [0, 1, 2, 3, 4, 5]
    rating_labels = ['1', '2', '3', '4', '5']
    # Create a new column 'rating_range' with the assigned bins
    df['rating_range'] = pd.cut(df['rating'], bins=rating_bins, labels=rating_labels)
    # Group by 'class' and 'rating_range' and count the ratings
    ratings = df.groupby(['class', 'rating_range'])['rating'].count().reset_index()
    ratings.rename(columns={'rating': 'Count'}, inplace=True)
    print("Sectioning the ratings completed")
    
    # Overall Ratings
    # Group by 'Class' and calculate the overall rating as a percentage
    class_ratings = df.groupby('class')['rating'].sum() / (df['rating'].max() * df.groupby('class')['rating'].count()) * 100
    class_ratings = class_ratings.reset_index()
    class_ratings.rename(columns={'rating': 'Percentage'}, inplace=True)
    print("Overall ratings completed")
    
    # Rating for last 90 days
    df['Date'] = df['time_stamp'].dt.date
    df['Date'] = pd.to_datetime(df['Date'])
    # Calculate the cutoff date for the last 90 days
    cutoff_date = df['Date'].max() - pd.DateOffset(days=90)
    # Filter the DataFrame to include only rows within the last 90 days
    filtered_df = df[df['Date'] >= cutoff_date]
    # Calculate the overall class rating for the last 90 days
    class_ratings_90_days = filtered_df.groupby('class')['rating'].sum() / (filtered_df['rating'].max() * filtered_df.groupby('class')['rating'].count()) * 100
    class_ratings_90_days = class_ratings_90_days.reset_index()
    print("Last three months rating completed")
    
    # Monthly performance of classes
    # Convert 'Date' column to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    # Extract the month and year from the 'Date' column
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    # Group by 'Year', 'Month', and 'Class', and calculate the mean Rating for each group
    month_df = df.groupby(['Year', 'Month', 'class'])['rating'].mean().reset_index()
    month_df.rename(columns={'rating': 'MonthlyRating'}, inplace=True)
    print("Monthly performance completed")
    
    # Upload all data to MongoDB
    client = MongoClient("mongodb+srv://yash:obi@cluster.l2wecvt.mongodb.net/?retryWrites=true&w=majority")
    db = client["test"]
    
    # Sectionized ratings for each class
    rating_collection = db["class_ratings"]
    rating_collection.drop()
    rating_collection = db["class_ratings"]
    ratings.reset_index(inplace = True)
    rating_data_dict = ratings.to_dict("records")
    rating_collection.insert_many(rating_data_dict)
    
    # Overall ratings for each class
    class_collection = db["overall_ratings"]
    class_collection.drop()
    class_collection = db["overall_ratings"]
    class_ratings.reset_index(inplace = True)
    class_data_dict = class_ratings.to_dict("records")
    class_collection.insert_many(class_data_dict)
    
    # Last three months for each class
    days_collection = db["three_months_ratings"]
    days_collection.drop()
    days_collection = db["three_months_ratings"]
    class_ratings_90_days.reset_index(inplace = True)
    days_data_dict = class_ratings_90_days.to_dict("records")
    days_collection.insert_many(days_data_dict)
    
    # Average monthly ratings for each class
    month_collection = db["monthly_ratings"]
    month_collection.drop()
    month_collection = db["monthly_ratings"]
    month_df.reset_index(inplace = True)
    months_data_dict = month_df.to_dict("records")
    month_collection.insert_many(months_data_dict)
    
    client.close()


# In[5]:


if __name__ == "__main__":
    # scheduler = BackgroundScheduler()
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(scheduled_function, 'cron', hour=18, minute = 10)
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(scheduled_function)
    
    results = future.result()
    
    print("Data analysis and uploading successful!")


# In[ ]:




