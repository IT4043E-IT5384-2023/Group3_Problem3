from dotenv import load_dotenv
load_dotenv()

from typing import *
from datetime import datetime, timedelta
from tweety import Twitter
import os
import json

from logger.logger import get_logger
logger = get_logger("crawler")

NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS"))

def get_twitter_session(account_id):
    assert account_id <= NUM_ACCOUNTS

    username = os.getenv(f"TWITTER_USERNAME_{account_id}")
    password = os.getenv(f"TWITTER_PASSWORD_{account_id}")
    key = os.getenv(f"TWITTER_KEY_{account_id}")

    app = Twitter(f"session_{username}")
    app.sign_in(username, password, extra=key)

    return app

def crawl_tweet_kol_last_day(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30,
) -> List[Dict]:
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
        List[Dict]: A list of dictionaries, each dictionary contains the data of a tweet.

    """

    if isinstance(keywords, str):
        keywords = [keywords]

    crawled_results = []

    for keyword in keywords:
        logger.info(f"Crawling for keyword {keyword}")
        search_str = f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets}"

        # Get yesterday's date
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        search_str += f" since:{yesterday_date}"

        # Get current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        search_str += f" until:{current_date}"
        
        all_tweets = app.search(search_str, pages = pages, wait_time = wait_time)
        for tweet in all_tweets:
            tweet_data = tweet.__dict__
            tweet_data['keyword_for_searching'] = keyword

            crawled_results.append(tweet_data)
        logger.info(f"Crawled {len(list(all_tweets))} tweets for keyword {keyword}")

    return crawled_results
