from dotenv import load_dotenv
import json
import os
from src.api_client import APIClient
from src.bq_loader import BQLoader

load_dotenv()

if __name__ == "__main__":

    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    api_client = APIClient("https://jsonplaceholder.typicode.com/")
    json_data = api_client.download_data("posts")
    
    filename = os.path.join(base_dir, "data.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=4)
        
    project_id = "p1422-477909"
    dataset_name = "SANDBOX_jsonplaceholder"
    table_name = "posts_raw"
    
    schema_file = os.path.join(base_dir, "schema.json")
    with open(schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)
        
    bq_loader = BQLoader(project_id)
    bq_loader.load_json_data(json_data,dataset_name,table_name,schema=schema)