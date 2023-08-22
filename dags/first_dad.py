try:
    # Importing all required libs
    from datetime import timedelta
    from datetime import datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    print("All Dag modules are ok......")
except ImportError as ie:
    print("Error {}!!!!".format(ie))


def first_function_execute():
    print("Hello World")
    return "Hello World"


with DAG(
    dag_id = "first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 8, 13)
    },
    catchup=False) as f:
    
    first_function_execute = PythonOperator(
        task_id = "first_function_execute",
        python_callable = first_function_execute,
    )
    