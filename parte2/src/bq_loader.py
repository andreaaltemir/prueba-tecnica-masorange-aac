from google.cloud import bigquery

class BQLoader:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        
    def load_json_data(self, data: list, dataset_name: str, table_name: str, schema: list = None, replace_table: bool = True):
        table_id = f"{self.project_id}.{dataset_name}.{table_name}"
        
        job_config = bigquery.LoadJobConfig()
        
        if schema:
            job_config.schema = schema
            job_config.autodetect = False
        else:
            job_config.autodetect = True
            
        if replace_table:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            
        job = self.client.load_table_from_json(data, table_id, job_config=job_config)
        job.result()
        print(f"{job.output_rows} rows loaded into {table_id}.")