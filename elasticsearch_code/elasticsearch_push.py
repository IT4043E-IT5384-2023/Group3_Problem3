import sys
sys.path.append(".")

import pandas as pd
from elasticsearch import Elasticsearch
import json
import argparse
import datetime

from dotenv import load_dotenv
load_dotenv()

import os
ELASTICSEARCH_CONFIG_DIR = os.getenv("ELASTICSEARCH_CONFIG_DIR")

from logger.logger import get_logger
logger = get_logger("elasticsearch")

def config_elasticsearch(config):
    with open(config) as f:
        config = json.load(f)
    es = Elasticsearch(config['elastic_server'],
                   basic_auth=(config['username'],config['password']))
    return es

def process_csv(input_path, es, index):
    df = pd.read_csv(input_path)
    new_df = df.copy()
    crawl_date = input_path[input_path.find("_")+1:input_path.find(".")]
    new_df["crawl_date"] = crawl_date
    new_df.drop(columns="join_time", inplace=True)
    new_df.to_json(f"users_{crawl_date}.json",index=False,orient="records")

    with open(f"users_{crawl_date}.json") as f:
        data = json.load(f)
    for instance in data:
        es.index(index=index, body=instance)
    logger.info('COMPLETED!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="users_2023-12-15.csv", help="input file path")
    parser.add_argument("--config", default="infra.json", help="config file path")
    parser.add_argument("--index", default = "g03_user_dates", help="Elasitcsearch index name")
    args = parser.parse_args()

    input_path = args.input
    config_path = args.config
    index = args.index

    es = config_elasticsearch(config_path)
    process_csv(input_path, es, index)
