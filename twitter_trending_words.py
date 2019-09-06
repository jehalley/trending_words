#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 07:44:37 2019

@author: JeffHalley
"""
from datetime import datetime
from nltk.corpus import stopwords
import os
import pandas as pd
import string

#update nltk stopwords
def make_stopwords_list():
    allStopwords = stopwords.words(('arabic',
 'azerbaijani',
 'danish',
 'dutch',
 'english',
 'finnish',
 'french',
 'german',
 'greek',
 'hungarian',
 'indonesian',
 'italian',
 'kazakh',
 'nepali',
 'norwegian',
 'portuguese',
 'romanian',
 'russian',
 'spanish',
 'swedish',
 'turkish'))
    newStopWords = ['like','rt','nan',' ','','im']
    allStopwords.extend(newStopWords)
    return allStopwords
    

#get list of all twitter archive files
def get_file_list(directory_path):
    file_list = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.bz2'):
                file_list.append(os.path.join(root, file))
    return file_list

#fsplit a tweet into list of words without tags, urls, stopwords or punctuation
def get_tweet_words_list(tweet):
    tweet_string = str(tweet).lower().split()
    filtered_tweet_string = [word for word in tweet_string if word not in allStopwords]
    tags_removed = [word for word in filtered_tweet_string if not word.startswith(('#','@','http', 'www'))]
    #remove punctuation                                                                               
    tweet_strings_list = [word.translate(str.maketrans('', '', string.punctuation)) for word in tags_removed]
    tweet_words_list = [word.translate(str.maketrans('', '', string.digits)) for word in tweet_strings_list] 
    return tweet_words_list

def get_tweet_dataframes(file_list):
    tweets_df = pd.DataFrame(columns=['date', 'date_hour','words'])
    for file_path in file_list:
        df = pd.read_json(file_path, orient = 'records', lines = True)
        filtered_df = df.filter(['created_at','text'])
        filtered_df['date'] = [datetime.date(d) for d in filtered_df['created_at']]
        filtered_df['date_hour'] = [d.replace(minute = 0, second = 0) for d in filtered_df['created_at']]
        filtered_df['words'] = filtered_df['text'].apply(get_tweet_words_list) 
        tweets_df = pd.concat([tweets_df, filtered_df], ignore_index=True, sort=True)
        tweets_df = tweets_df.filter(['date', 'date_hour','words'])
    return tweets_df
 
        
def get_trending_words_df(tweets_df):
    rows = list()
    for row in tweets_df[['date', 'words']].iterrows():
        r = row[1]
        for word in r.words:
            rows.append((r.date, word))
    words = pd.DataFrame(rows, columns=['date', 'word'])
    word_counts_by_date = words.groupby(['date','word']).size()\
        .to_frame()\
        .reset_index()
    word_counts_by_date.columns = ['date','word','count']
    #find total counts per date so count can be converted to frequency
    sum_of_counts_per_date = word_counts_by_date.groupby('date')['count']\
    .sum()\
    .to_frame()\
    .reset_index()
    sum_of_counts_per_date.columns=['date','count_sum']
    word_counts_by_date = pd.merge(word_counts_by_date,sum_of_counts_per_date,left_on=['date'], right_on = ['date'], how = 'left')
    word_counts_by_date['word_frequency'] = word_counts_by_date['count']/word_counts_by_date['count_sum']
    word_frequencies_by_date = word_counts_by_date[['date','word','word_frequency']]
    reshaped_df = word_frequencies_by_date.pivot(index='word', columns='date', values='word_frequency')
    freq_change_per_day_for_all_days = pd.DataFrame(columns=['word','date', 'frequency_change'])
    for date in range(1,len(reshaped_df.columns)):
        freq_change_during_day = reshaped_df[[reshaped_df.columns[date-1],reshaped_df.columns[date]]].copy()
        freq_change_during_day['frequency_change'] = freq_change_during_day[freq_change_during_day.columns[1]] - freq_change_during_day[freq_change_during_day.columns[0]]
        freq_change_during_day['date'] = freq_change_during_day.columns[1]
        freq_change_during_day.reset_index(level=0, inplace=True)
        freq_change_filtered = freq_change_during_day.filter(['word','date','frequency_change'])
        freq_change_per_day_for_all_days = pd.concat([freq_change_per_day_for_all_days, freq_change_filtered], ignore_index=True, sort=True)
    freq_change_per_day_for_all_days = freq_change_per_day_for_all_days.sort_values('frequency_change', ascending = False)
    freq_change_per_day_for_all_days = freq_change_per_day_for_all_days.reset_index()
    top_100_trending_words = freq_change_per_day_for_all_days.filter(['word','date'])[0:100]   
    return top_100_trending_words


def get_trending_word_frequencies_by_hour(tweets_df):
    rows = list()
    for row in tweets_df[['date_hour', 'words']].iterrows():
        r = row[1]
        for word in r.words:
            rows.append((r.date_hour, word))
    words = pd.DataFrame(rows, columns=['date_hour', 'word'])
    word_counts_by_hour = words.groupby(['date_hour','word']).size()\
        .to_frame()\
        .reset_index()
    word_counts_by_hour.columns = ['date_hour','word','count']
    #find total counts per date so count can be converted to frequency
    sum_of_counts_per_hour = word_counts_by_hour.groupby('date_hour')['count']\
    .sum()\
    .to_frame()\
    .reset_index()
    sum_of_counts_per_hour.columns=['date_hour','count_sum']
    word_counts_by_hour = pd.merge(word_counts_by_hour,sum_of_counts_per_hour,left_on=['date_hour'], right_on = ['date_hour'], how = 'left')
    word_counts_by_hour['word_frequency'] = word_counts_by_hour['count']/word_counts_by_hour['count_sum']
    word_frequencies_by_hour = word_counts_by_hour[['date_hour','word','word_frequency']].copy()
    word_frequencies_by_hour['date']= [datetime.date(d) for d in word_frequencies_by_hour['date_hour']]
    #get the data just for the trending words
    trending_word_frequencies_by_hour = trending_words_df.merge(word_frequencies_by_hour, how = 'inner', on = ['date', 'word'])
    return trending_word_frequencies_by_hour
    
           
allStopwords = make_stopwords_list()
directory_path = '/Users/JeffHalley/Downloads/2018_copy'
file_list = get_file_list(directory_path)      
tweets_df = get_tweet_dataframes(file_list)
trending_words_df = get_trending_words_df(tweets_df)
trending_word_frequencies_by_hour = get_trending_word_frequencies_by_hour(tweets_df)
