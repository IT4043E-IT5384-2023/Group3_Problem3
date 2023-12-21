import json
import random

if __name__ == "__main__":
    example_data = {"a": 1, "b": 2}
    
    index = random.randint(0, 100)

    with open(f"airflow_dags/example_data_{index}.json", 'w') as f:
        json.dump(example_data, f)
