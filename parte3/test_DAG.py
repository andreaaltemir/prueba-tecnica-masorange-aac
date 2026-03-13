from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models import BaseOperator
from datetime import datetime, timedelta

class TimeDiff(BaseOperator):
    def __init__(self, diff_date, **kwargs) -> None:
        super().__init__(**kwargs)
        self.diff_date = diff_date
    
    def execute(self, context):
        current_date = datetime.now()
        difference = current_date - self.diff_date
        print(f"Diff date: {self.diff_date}\nCurrent date: {current_date}\nTime difference: {difference}")
        return difference
    
N = 6
diff_date = datetime(2026,3,10)

with DAG(
    dag_id = "test",
    schedule="0 3 * * *",
    catchup = False,
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(1900, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    }
) as dag:
    
    start = DummyOperator(
        task_id = "start"
    )

    end = DummyOperator(
        task_id = "end"
    )

    even_tasks = []
    odd_tasks = []
    for n in range(1,N+1):
        task = DummyOperator(
            task_id = f"task_{n}"
        )
        if n%2 == 0:
            even_tasks.append(task)
        else:
            odd_tasks.append(task)

    for task in even_tasks:
       odd_tasks >> task

    time_diff = TimeDiff(
        task_id = "time_diff",
        diff_date = diff_date
    )

    start >> odd_tasks
    even_tasks >> time_diff >> end


'''
Un hook es una interfaz que permite a Python conectarse a plataformas externas de forma sencilla, 
sin necesidad de utilizar explícitamente APIs o librerías concretas.

Las conexiones son los datos necesarios para conectarse a esas plataformas externas (usuarios, contraseñas, etc).
'''