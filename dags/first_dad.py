try:
    # Importing all required libs
    from datetime import timedelta
    from datetime import datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    print("All Dag modules are ok......")
except ImportError as ie:
    print("Error {}!!!!".format(ie))


def first_function_execute(**context):
    print("First Function Execute.....")
    print(type(context['ti']), context['ti'], dir(context['ti']), sep = "\n****************")
    context['ti'].xcom_push(key = "name", value = "Jaswanthraj R")

def second_function_execute(**context):
    instance = context['ti'].xcom_pull(key = "name")
    print("I am in Second Function Execute got value: {} from function 1......".format(instance))


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
        provide_context=True,
        
    )
    second_function_execute = PythonOperator(
        task_id = "second_function_execute",
        python_callable = second_function_execute,
        provide_context=True,
    )
    
first_function_execute >> second_function_execute