import requests
from tweety import Twitter
import pandas as pd
from tweet_kol_crawler_v2 import convert_to_json
import argparse
import time

def get_follow(screenname,listed = [], cursor = None):
    url = 'https://twitter-api45.p.rapidapi.com/followers.php'
    querystring = {"screenname":screenname}
    if cursor:
        querystring["cursor"] = cursor
    headers = {
        "X-RapidAPI-Key": "008eb73edcmsh4bb60f51534fc3ep142f9ajsnb186fcc18eb6",
        "X-RapidAPI-Host": 'twitter-api45.p.rapidapi.com',
    }
    response = requests.get(url, headers=headers, params=querystring)
    response = response.json()

    for fl in response['followers']:
        listed.append(fl['screen_name'])
    try:
        cursor = response['next_cursor']
        get_follow(screenname,cursor)
    except:
        return listed
    
def unpack_all_followers(app,path):
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

    fl_list = []
    fl_data = []
    df = pd.read_csv(path)
    kol_list = df['screen_name'].tolist()
    for kol in kol_list:
        gfl = get_follow(kol)
        if gfl:
            fl_list.extend(gfl)

    for follower in fl_list:
        try:
            user = app.get_user_info(follower)
            #time.sleep(2)
            fl_data.append(user)

            user_id.append(user['id'])
            username.append(user['username'])
            name.append(user['name'])
            screen_name.append(user['screen_name'])
            created_at.append(user['created_at'])
            is_verified.append(user['verified'])
            media_count.append(user['media_count'])
            listed_count.append(user['listed_count'])
            statuses_count.append(user['statuses_count'])
            favourites_count.append(user['favourites_count'])
            followers_count.append(user['followers_count'])
            normal_followers_count.append(user['normal_followers_count'])
            fast_followers_count.append(user['fast_followers_count'])
            friends_count.append(user['friends_count'])
            possibly_sensitive.append(user['possibly_sensitive'])
            profile_url.append(user['profile_url'])
        except:
            continue

    followers_table_df = pd.DataFrame({
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

    convert_to_json(fl_data, 'followers.json')
    return followers_table_df

if __name__=="__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", type=str, help="Twitter username")
    parser.add_argument("--password", type=str, help="Twitter password")
    parser.add_argument("--key", type=str, default='0', help="Twitter key")
    args = parser.parse_args()

    app = Twitter('session')
    #app.sign_in(args.username, args.password)#, extra=args.key)

    followers = unpack_all_followers(app, 'data/kols_table_btc_mf100_mr10_p10_since2023-11-14_until2023-11-15.csv')
    followers.to_csv('data/followers.csv', index=False, encoding='utf-8')
