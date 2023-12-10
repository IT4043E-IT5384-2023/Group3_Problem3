import json
import os

def convert_to_json(data, json_filename):
    # Open the JSON file in write mode
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Write the data to the JSON file
        for tweet in data:
          json.dump(tweet, json_file, ensure_ascii=False, indent=4, default = str)
