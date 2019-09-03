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
from pandas import Timestamp, Series, date_range
import re
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

#function to split a tweet into list of words without tags, urls, stopwords or punctuation
def get_tweet_words_list(tweet):
    tweet_string = str(tweet).lower().split()
    filtered_tweet_string = [word for word in tweet_string if word not in allStopwords]
    tags_removed = [word for word in filtered_tweet_string if not word.startswith(('#','@','http', 'www'))]
    #remove punctuation                                                                               
    tweet_strings_list = [word.translate(str.maketrans('', '', string.punctuation)) for word in tags_removed]
    tweet_words_list = [word.translate(str.maketrans('', '', string.digits)) for word in tweet_strings_list] 
    return tweet_words_list
            
               
def get_word_frequencies_by_date(file_list):
    tweets_word_counts = pd.DataFrame(columns=['date', 'word','count'])
    for file_path in file_list:
        df = pd.read_json(file_path, orient = 'records', lines = True)
        filtered_df = df.filter(['created_at','text'])
        filtered_df['date'] = [datetime.date(d) for d in filtered_df['created_at']]
        filtered_df['words'] = filtered_df['text'].apply(get_tweet_words_list) 
        rows = list()
        for row in filtered_df[['date', 'words']].iterrows():
            r = row[1]
            for word in r.words:
                rows.append((r.date, word))
        words = pd.DataFrame(rows, columns=['date', 'word'])
        word_counts_by_date = words.groupby(['date','word']).size()\
            .to_frame()\
            .reset_index()
        word_counts_by_date.columns = ['date','word','count']
        if len(tweets_word_counts.index) == 0:
            tweets_word_counts = pd.concat([tweets_word_counts, word_counts_by_date], ignore_index=True, sort=True)
        else:
            tweets_word_counts = tweets_word_counts.groupby(['date', 'word']).sum().add(word_counts_by_date.groupby(['date', 'word']).sum(), fill_value=0).reset_index()
        #find total counts per date so count can be converted to frequency
    sum_of_counts_per_date = tweets_word_counts.groupby('date')['count']\
    .sum()\
    .to_frame()\
    .reset_index()
    sum_of_counts_per_date.columns=['date','count_sum']
    tweets_word_counts = pd.merge(tweets_word_counts,sum_of_counts_per_date,left_on=['date'], right_on = ['date'], how = 'left')
    tweets_word_counts['word_frequency'] = tweets_word_counts['count']/tweets_word_counts['count_sum']
    tweets_word_frequencies = tweets_word_counts[['date','word','word_frequency']]
    return tweets_word_frequencies
        
    
def get_trending_words_dataframe(word_frequencies_by_date):
    max_date = word_frequencies_by_date['date'].max()
    min_date = word_frequencies_by_date['date'].min()
    reshaped_df = word_frequencies_by_date.pivot(index='word', columns='date', values='word_frequency')
    reshaped_df['date_of_max_freq'] = reshaped_df.idxmax(axis=1)
    trending_words_df = df2.loc[df2['Max'] == max_date].fillna(0)
    trending_words_df['frequency_change'] = trending_words_df[max_date] - trending_words_df[min_date]
    return trending_words_df
    

allStopwords = make_stopwords_list()
directory_path = '/Users/JeffHalley/Downloads/2018_copy'
file_list = get_file_list(directory_path)      
word_frequencies_by_date = get_word_frequencies_by_date(file_list)
word_frequencies_by_date.to_csv('aggregate_and_frequency_once_tweets_df.csv')
trending_words_df = get_trending_words_dataframe(word_frequencies_by_date)
