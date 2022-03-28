from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG

dag = DAG(
    dag_id="Transformacao_BR",
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval="0 */4 * * *",
    #schedule_interval="* * * * *",
    max_active_tasks=100
)

Transformacao_dbt = BashOperator(
    task_id= f"dbt",
    bash_command= 'cd ~/ELT_Recode; dbt run',
    dag=dag

)
