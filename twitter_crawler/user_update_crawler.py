import pandas as pd
import argparse
from tweety import Twitter
import tweety.exceptions_ as te
import datetime
from tqdm import tqdm

def get_dataframe(input_path):
    df = pd.read_csv(input_path)
    return df

def crawl_follower(df):
    #id,name,username,bio,location,url,join_date,join_time,tweets,following,followers,likes,media,private,verified,profile_image_url,background_image
    app = Twitter("session")
    update_dict = {"id":[], "name":[],
                    "username":[],
                    "bio":[],
                    "location":[],
                    "url":[],
                    "join_date":[],
                    "join_time":[],
                    "tweets":[],
                    "following":[],
                    "followers":[],
                    "likes":[],
                    "media":[],
                    "private":[],
                    "verified":[],
                    "profile_image_url":[],
                    "background_image":[]}
    
    tmp_success = False
    private = 0
    for _,row in tqdm(df.iterrows()):
        uname = row["username"]
        try:
            user = app.get_user_info(uname)
            tmp_success = True
        except te.UnknownError:
            app = Twitter("session")
            try:
                user = app.get_user_info(uname)
                tmp_success = True
            except:
                private += 1
                tmp_success = False
        except:
            private += 1
            tmp_success = False

        if tmp_success:
            update_dict["id"].append(user.id)
            update_dict["name"].append(user.name)
            update_dict["username"].append(user.username)
            update_dict["bio"].append(user.bio)
            update_dict["location"].append(user.location)
            update_dict["url"].append(user.profile_url)
            update_dict["join_date"].append(user.created_at.strftime("%Y-%m-%d"))
            update_dict["join_time"].append(user.created_at.strftime("%H:%M:%S"))
            update_dict["tweets"].append(user.statuses_count)
            update_dict["following"].append(user.friends_count)
            update_dict["followers"].append(user.followers_count)
            update_dict["likes"].append(user.favourites_count)
            update_dict["media"].append(user.media_count)
            update_dict["private"].append(1 if user.protected else 0)
            update_dict["verified"].append(1 if user.verified else 0)
            update_dict["profile_image_url"].append(user.profile_image_url_https)
            update_dict["background_image"].append(user.profile_banner_url)
        else:
            update_dict["id"].append(row["id"])
            update_dict["name"].append(row["name"])
            update_dict["username"].append(row["username"])
            update_dict["bio"].append(row["bio"])
            update_dict["location"].append(row["location"])
            update_dict["url"].append(row["url"])
            update_dict["join_date"].append(row["join_date"])
            update_dict["join_time"].append(row["join_time"])
            update_dict["tweets"].append(row["tweets"])
            update_dict["following"].append(row["following"])
            update_dict["followers"].append(row["followers"])
            update_dict["likes"].append(row["likes"])
            update_dict["media"].append(row["media"])
            update_dict["private"].append(1)
            update_dict["verified"].append(row["verified"])
            update_dict["profile_image_url"].append(row["profile_image_url"])
            update_dict["background_image"].append(row["background_image"])

    update_df = pd.DataFrame.from_dict(update_dict)
    time = datetime.datetime.today().strftime('%Y-%m-%d')
    update_df.to_csv(f"users_{time}.csv",index=False)
    print(f"Completed! {private} private accounts found!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="input file path")
    args = parser.parse_args()

    input_df = get_dataframe(args.input)
    crawl_follower(input_df)