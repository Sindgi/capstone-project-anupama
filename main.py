import streamlit as st
import pandas as pd
import snscrape.modules.twitter as sntwitter
from pymongo import MongoClient

client = MongoClient()
db = client['twitter_snscraper']


def scrape_tweets(query, start_date, end_date, num_tweets):
    tweets = []
    for i, tweet in enumerate(
            sntwitter.TwitterSearchScraper(f'{query} since:{start_date} until:{end_date}').get_items()):
        if i >= num_tweets:
            break
        tweets.append({'date': tweet.date.strftime('%Y-%m-%d %H:%M:%S'),
                       'id': tweet.id, 'url': tweet.url,
                       'content': tweet.content,
                       'user': tweet.user.username,
                       'reply_count': tweet.replyCount,
                       'retweet_count': tweet.retweetCount,
                       'language': tweet.lang,
                       'source': tweet.sourceLabel,
                       'like_count': tweet.likeCount})
    return tweets


def store_tweets(query, tweets):
    db[query].insert_one({'Scraped word': query, 'scraped_date': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'scraped_data': tweets})


def get_tweets(query):
    return pd.DataFrame(db[query].find({}, {'_id': 0}))


def download_csv(query):
    df = get_tweets(query)
    csv = df.to_csv(index=False)
    return csv


def download_json(query):
    df = get_tweets(query)
    json = df.to_json(orient='records')
    return json


# Set up Streamlit app
st.title('Twitter Scraper using snscraper')
query = st.text_input('Enter a keyword or hashtag')
start_date = st.date_input('Start date', value=pd.Timestamp('2021-01-01'))
end_date = st.date_input('End date', value=pd.Timestamp.now())
num_tweets = st.number_input('Number of tweets to scrape', min_value=1, max_value=1000)
submit = st.button('Scrape tweets')

if submit:
    tweets = scrape_tweets(query, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), num_tweets)
    store_tweets(query, tweets)
    st.write('Scraped data:')
    j = pd.DataFrame(tweets)
    st.write(j)

    st.write('Download data:')
    # csv download
    df1=j
    csv = df1.to_csv(index=False)
    st.download_button('Download CSV', data=csv, file_name=f'{query}.csv', mime='text/csv')

    # Download tweets from MongoDB as JSON
    df2=j
    json = df2.to_json(orient='records')
    st.download_button('Download JSON', data=json, file_name=f'{query}.json', mime='application/json')

    upload = st.button('Upload data to MongoDB')
    if upload:
        store_tweets(query, tweets)
        st.write('Data uploaded to MongoDB')