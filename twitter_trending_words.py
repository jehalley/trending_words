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
    extensions = ('.bz2','.json')
    file_list = []
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith(extensions):
                file_list.append(os.path.join(root, file))
    return file_list

#fsplit a tweet into list of words without tags, urls, stopwords or punctuation
def get_words_list(comment):
    comment_string = str(comment).lower().split()
    filtered_comment_string = [word for word in comment_string if word not in allStopwords]
    tags_removed = [word for word in filtered_comment_string if not word.startswith(('#','@','http', 'www'))]
    #remove punctuation                                                                               
    comment_strings_list = [word.translate(str.maketrans('', '', string.punctuation)) for word in tags_removed]
    comment_words_list = [word.translate(str.maketrans('', '', string.digits)) for word in comment_strings_list] 
    return comment_words_list

def get_reddit_dataframes(reddit_file_list):
    reddit_df = pd.DataFrame(columns=['date', 'date_hour','words'])
    for file_path in reddit_file_list:
        df = pd.read_json(file_path, orient = 'records', lines = True)
        filtered_df = df.filter(['created_utc','body'])
        filtered_df['date_hour'] = [datetime.strptime(datetime.utcfromtimestamp(d).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S' ) for d in filtered_df['created_utc']]
        filtered_df['date_hour'] = [d.replace(minute = 0, second = 0) for d in filtered_df['date_hour']]
        filtered_df['date']= [datetime.date(d) for d in filtered_df['date_hour']]
        filtered_df['words'] = filtered_df['body'].apply(get_words_list) 
        reddit_df = pd.concat([reddit_df, filtered_df], ignore_index=True, sort=True)
        reddit_df = reddit_df.filter(['date', 'date_hour','words'])
    return reddit_df
        
 
#def get_tweet_dataframes(file_list):
#    tweets_df = pd.DataFrame(columns=['date', 'date_hour','words'])
#    for file_path in file_list:
#        df = pd.read_json(file_path, orient = 'records', lines = True)
#        filtered_df = df.filter(['created_at','text'])
#        filtered_df['date'] = [datetime.date(d) for d in filtered_df['created_at']]
#        filtered_df['date_hour'] = [d.replace(minute = 0, second = 0) for d in filtered_df['created_at']]
#        filtered_df['words'] = filtered_df['text'].apply(get_words_list) 
#        tweets_df = pd.concat([tweets_df, filtered_df], ignore_index=True, sort=True)
#        tweets_df = tweets_df.filter(['date', 'date_hour','words'])
#    return tweets_df
        
def get_trending_words_df(reddit_df):
    rows = list()
    for row in reddit_df[['date', 'words']].iterrows():
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

def get_trending_word_frequencies_by_hour(reddit_df):
    rows = list()
    for row in reddit_df[['date_hour', 'words']].iterrows():
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

def get_reddit_file_list(reddit_directory_path):
    file_list = []
    for root, dirs, files in os.walk(reddit_directory_path):
        for file in files:
            if file.endswith('.json'):
                file_list.append(os.path.join(root, file))
    return file_list

def get_twitter_word_frequencies_by_hour(twitter_file_list):
    tweets_word_counts = pd.DataFrame(columns=['date_hour', 'word','count'])
    #get word counts by date for each json file
    for file_path in twitter_file_list:
        df = pd.read_json(file_path, orient = 'records', lines = True)
        filtered_df = df.filter(['created_at','text'])
        filtered_df['date_hour'] = [d.replace(minute = 0, second = 0) for d in filtered_df['created_at']]
        filtered_df['words'] = filtered_df['text'].apply(get_words_list) 
        rows = list()
        for row in filtered_df[['date_hour', 'words']].iterrows():
            r = row[1]
            for word in r.words:
                rows.append((r.date_hour, word))
        words = pd.DataFrame(rows, columns=['date_hour', 'word'])
        word_counts_by_date = words.groupby(['date_hour','word']).size()\
            .to_frame()\
            .reset_index()
        word_counts_by_date.columns = ['date_hour','word','count']
        if len(tweets_word_counts.index) == 0:
            tweets_word_counts = pd.concat([tweets_word_counts, word_counts_by_date], ignore_index=True, sort=True)
        else:
            tweets_word_counts = tweets_word_counts.groupby(['date_hour', 'word']).sum().add(word_counts_by_date.groupby(['date_hour', 'word']).sum(), fill_value=0).reset_index()
    tweets_word_counts = tweets_word_counts.drop_duplicates(keep = 'first')
    #find total counts per date so count can be converted to frequency
    sum_of_counts_per_date = tweets_word_counts.groupby('date_hour')['count']\
    .sum()\
    .to_frame()\
    .reset_index()
    sum_of_counts_per_date.columns=['date_hour','count_sum']
    tweets_word_counts = pd.merge(tweets_word_counts,sum_of_counts_per_date,left_on=['date_hour'], right_on = ['date_hour'], how = 'left')
    tweets_word_counts['word_frequency'] = tweets_word_counts['count']/tweets_word_counts['count_sum']
    tweets_word_frequencies = tweets_word_counts[['date_hour','word','word_frequency']]
    return tweets_word_frequencies  

def get_joint_reddit_and_twitter_freqs(trending_word_frequencies_by_hour, twitter_word_frequencies_by_hour):
    joint_reddit_and_twitter_freqs = trending_word_frequencies_by_hour.merge(twitter_word_frequencies_by_hour, how = 'inner', on = ['date_hour', 'word'])
    #change column names for frequencies
    return joint_reddit_and_twitter_freqs 
      
allStopwords = make_stopwords_list()

reddit_directory_path = '/Users/JeffHalley/Downloads/RC_2018-07_test'
reddit_file_list = get_file_list(reddit_directory_path)
reddit_df = get_reddit_dataframes(reddit_file_list)
reddit_df.to_csv('reddit_df.csv')
trending_words_df = get_trending_words_df(reddit_df)
trending_words_df.to_csv('reddit_trending_words.csv')
trending_word_frequencies_by_hour = get_trending_word_frequencies_by_hour(reddit_df)
trending_word_frequencies_by_hour.to_csv('reddit_trending_words_freqs_by_hour.csv')

twitter_directory_path = '/Users/JeffHalley/Downloads/2018_copy'
twitter_file_list = get_file_list(twitter_directory_path)
twitter_word_frequencies_by_hour = get_twitter_word_frequencies_by_hour(twitter_file_list)      

joint_reddit_and_twitter_freqs= get_joint_reddit_and_twitter_freqs(trending_word_frequencies_by_hour, twitter_word_frequencies_by_hour)

