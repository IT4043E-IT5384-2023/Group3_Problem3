import requests
from tweety import Twitter
import pandas as pd
from utils import convert_to_json
import argparse
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()
import os
X_RapidAPI_Key = os.getenv("X-RapidAPI-Key")
X_RapidAPI_Host = os.getenv("X-RapidAPI-Host")

def get_follow(screenname):
    l = []
    url = 'https://twitter-api45.p.rapidapi.com/followers.php'
    querystring = {"screenname":screenname}
    headers = {
        "X-RapidAPI-Key": X_RapidAPI_Key,
        "X-RapidAPI-Host": X_RapidAPI_Host,
    }
    
    iter = 0
    while True:
        response = requests.get(url, headers=headers, params=querystring)
        response = response.json()
        
        if response['status'] == 'ok':
            # append followers to list
            for fl in response['followers']:
                l.append(fl['screen_name'])
            
            # update cursor:
            querystring["cursor"] = response.get("next_cursor", None)
            if not querystring["cursor"]:
                break
            # print(f"\t{screenname} iter {iter}: {len(response['followers'])} followers")
            iter += 1 
        else:
            break
    
    print(f"\tCrawled {len(l)} followers for user {screenname}")
    return l
    
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
        print(f"Get followers of {kol}...")
        gfl = get_follow(kol)
        if gfl:
            fl_list.extend(gfl)

    print(f"Total number of user to get info: {len(fl_list)}")
    print("Get followers' info...")
    for follower in tqdm(fl_list):
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

    followers = unpack_all_followers(app, 'demo_data/test.csv')
    followers.to_csv('demo_data/followers.csv', index=False, encoding='utf-8')
