from typing import List, Union
from tweety import Twitter
import pandas as pd
import argparse
import os
import json
import sys
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from utils import read_yaml

# Specify the JSON filename
def convert_to_json(data, json_filename):
    # Open the JSON file in write mode
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Write the data to the JSON file
        for tweet in data:
          json.dump(tweet, json_file, ensure_ascii=False, indent=4, default = str)


def crawl_tweet_kol(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30,
    since: str = None,
    until: str = None
) -> List[pd.DataFrame]:
    
    """
    Crawl tweets and KOL accounts from Twitter.

    Args:
        app (TwitterApp): The Twitter app instance used for authentication.
        keywords (Union[str, List[str]]): Keywords used to search for tweets.
            Can be a single string or a list of strings.
        min_faves (int): Minimum number of likes for a tweet to be included.
        min_retweets (int): Minimum number of retweets for a tweet to be included.
        pages (int): Number of scroll down refreshing times during the crawling.
        wait_time (int): Interval to wait between 2 pages in seconds.

    Returns:
        List[pd.DataFrame, pd.DataFrame]: A tuple containing two pandas DataFrames.
            The first DataFrame is for tweets data, and the second is for KOLs data.

    """
    
    # 1st table columns
    keyword_used = []
    tweet_id = []
    tweet_body = []
    date = []
    is_sensitive = []
    like_counts = []
    reply_counts = []
    quote_counts = []
    bookmark_counts = []
    hashtags = []
    author_id = []
    views = []
    retweet_counts = []
    url = []

    # 2nd table columns
    user_id = []
    username = []
    name = []
    created_at = []
    is_verified = []
    media_count = []
    statuses_count = []
    favourites_count = []
    followers_count = []
    profile_url = []
    possibly_sensitive = []
    screen_name = []
    listed_count = []
    normal_followers_count = []
    fast_followers_count = []
    friends_count = []

    for keyword in keywords:
        print(f"Crawling for keyword {keyword}")
        search_str = f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets}"
        if since:
            search_str += f" since:{since}"
        if until:
            search_str += f"until:{until}"
        all_tweets = app.search(search_str, pages = pages, wait_time = wait_time)
        if since and until:
            convert_to_json(all_tweets,f"{keyword}_{since}_{until}.json")
        else:
            convert_to_json(all_tweets,f"{keyword}.json")
        for tweet in all_tweets:
            tweet_data = tweet.__dict__
            author_data = tweet['author'].__dict__
            
            # 1st table data
            keyword_used.append(keyword)
            tweet_id.append(tweet_data['id'])
            tweet_body.append(tweet_data['tweet_body'])
            date.append(tweet_data['date'])
            is_sensitive.append(tweet_data['is_sensitive'])
            like_counts.append(tweet_data['likes'])
            reply_counts.append(tweet_data['reply_counts'])
            quote_counts.append(tweet_data['quote_counts'])
            bookmark_counts.append(tweet_data['bookmark_count'])
            hashtags.append(tweet_data['hashtags'])
            author_id.append(author_data['id'])
            views.append(tweet_data['views'])
            retweet_counts.append(tweet_data['retweet_counts'])
            url.append(tweet_data['url'])
            
            # 2nd table data
            user_id.append(author_data['id'])
            username.append(author_data['username'])
            name.append(author_data['name'])
            screen_name.append(author_data['screen_name'])
            created_at.append(author_data['created_at'])
            is_verified.append(author_data['verified'])
            media_count.append(author_data['media_count'])
            listed_count.append(author_data['listed_count'])
            statuses_count.append(author_data['statuses_count'])
            favourites_count.append(author_data['favourites_count'])
            followers_count.append(author_data['followers_count'])
            normal_followers_count.append(author_data['normal_followers_count'])
            fast_followers_count.append(author_data['fast_followers_count'])
            friends_count.append(author_data['friends_count'])
            possibly_sensitive.append(author_data['possibly_sensitive'])
            profile_url.append(author_data['profile_url'])
            
    first_table_df = pd.DataFrame({
        'keyword_used': keyword_used,
        'tweet_id': tweet_id,
        'tweet_body': tweet_body,
        'date': date,
        'is_sensitive': is_sensitive,
        'like_counts': like_counts,
        'reply_counts': reply_counts,
        'quote_counts': quote_counts,
        'bookmark_counts': bookmark_counts,
        'hashtags': hashtags,
        'author_id': author_id,
        'views': views,
        'retweet_counts': retweet_counts,
        'url': url

    })

    second_table_df = pd.DataFrame({
        'user_id': user_id,
        'username': username,
        'name': name,
        'screen_name' : screen_name,
        'created_at': created_at,
        'is_verified': is_verified,
        'listed_count': listed_count,
        'media_count': media_count,
        'statuses_count': statuses_count,
        'favourites_count': favourites_count,
        'followers_count': followers_count,
        'normal_followers_count': normal_followers_count,
        'friends_count': friends_count,
        'possibly_sensitive' : possibly_sensitive,
        'fast_followers_count' : fast_followers_count,
        'profile_url': profile_url
    })

    second_table_df.drop_duplicates(keep='first', inplace=True)
    second_table_df.reset_index(drop=True, inplace=True)
    
    return [first_table_df, second_table_df]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", type=str, help="Twitter username")
    parser.add_argument("--password", type=str, help="Twitter password")
    parser.add_argument("--key", type=str, help="Twitter key")
    parser.add_argument("--keywords", type=str, help="Keywords used to search for tweets, seperated by comma")
    parser.add_argument("--min_faves", type=int, default=100, help="Minimum number of likes for a tweet to be included")
    parser.add_argument("--min_retweet", type=int, default=10, help="Minimum number of retweets for a tweet to be included")
    parser.add_argument("--pages", type=int, default=20, help="Number of scroll down refreshing times during the crawling")
    parser.add_argument("--wait_time", type=int, default=30, help="Interval to wait between 2 pages in seconds")
    parser.add_argument("--since", type=str, help="The date to start crawling from, format: YYYY-MM-DD")
    parser.add_argument("--until", type=str, help="The date to stop crawling, format: YYYY-MM-DD")
    args = parser.parse_args()


    username = args.username
    password = args.password
    key = args.key
    min_faves = args.min_faves
    min_retweets = args.min_retweet
    pages = args.pages
    wait_time = args.wait_time
    since = args.since
    until = args.until

    keywords = args.keywords.split(",")
    app = Twitter("session")
    app.sign_in(username, password,extra = key)
    tweets_df, kols_df = crawl_tweet_kol(app, keywords, min_faves, min_retweets, pages, wait_time, since, until)

    name_extension = f'{args.keywords}_mf{min_faves}_mr{min_retweets}'
    if since:
        name_extension += f'_since{since}'
    if until:
        name_extension += f'_until{until}'

    tweets_df.to_csv(f"data/tweets_{name_extension}.csv", index=False, encoding='utf-8')
    kols_df.to_csv(f"data/kols_table_{name_extension}.csv", index=False, encoding='utf-8')
    
