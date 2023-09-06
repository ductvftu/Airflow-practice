from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.filesystem import FileSensor  
from datetime import datetime, timedelta
import pandas as pd

# Define DAG parameters
default_args = {
    'owner': 'ductv50',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 28, 11, 50),  # Start time for the DAG
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'filtering_customer_consumption_backup',
    default_args=default_args,
    schedule_interval=None,  # Set to None to manually trigger the DAG, This suppose to be @Daily
    catchup=False,  # Prevent backfilling old data
    max_active_runs=1,  # Allow only one active DAG run at a time
)

# Config global variables
postgres_hook = PostgresHook(postgres_conn_id='Postgress_1')
execution_date_formatted = datetime.now().strftime('%Y%m%d')

# Task to check for the raw data file
file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/opt/airflow/dags/resources/rawdata/consumption_yyyymmdd.csv',
    poke_interval=300,  # Interval in seconds to check for the file (5 minutes)
    timeout=900,  # Maximum time to wait for the file (15 minutes)
    mode='poke',  # 'poke' mode actively polls for the file
    fs_conn_id= "fs1",
    dag=dag,
    # Set the execution window between 11:45 PM and 12:00 AM
    execution_timeout=timedelta(hours=0, minutes=15),
)

#Task to create table if not exist
Table_Names = [f"consumption_alcoholic_{execution_date_formatted}",
               f"consumption_cereals_bakery_{execution_date_formatted}",
               f"consumption_meats_poultry_{execution_date_formatted}"]

sql_create_table = ""

for table_name in Table_Names:
    sql_create_table += f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            category VARCHAR(255),
            sub_category VARCHAR(255),
            aggregation_date DATE,
            millions_of_dollar INT,            
            pipeline_exc_datetime TIMESTAMP
        );
    """
#drop table so that everytime rerun for debug, the records wont accumulated

# Task to copy raw data and transform
def Transform_data_and_insert(**kwargs):
    # Read the data
    df = pd.read_csv('/opt/airflow/dags/resources/rawdata/consumption_yyyymmdd.csv')
    # Convert 'Month' column to datetime
    df['Month'] = pd.to_datetime(df['Month'], errors='coerce', format='%d/%m/%Y').dt.strftime('%Y/%m/%d')
    # Rename the df to suit posgres columns
    new_column_names = {
        'Category': 'category',
        'Sub-Category': 'sub_category',
        'Month': 'aggregation_date',
        'Millions of Dollars': 'millions_of_dollar',
    } 
    df = df.rename(columns=new_column_names) 
    # Get execution timestamp
    df['pipeline_exc_datetime'] = kwargs['ts']
    # Create 3 separate df for 3 tables:

    dataframes = [df[df['category'] == 'Alcoholic beverages'],
                  df[df['category'] == 'Cereals and bakery products'],
                  df[df['category'] == 'Meats and poultry']]

    df_count_dict = {}
    postgres_hook = PostgresHook(postgres_conn_id='Postgress_1')
    for df, table_name in zip(dataframes, Table_Names):
        df_count = df.shape[0]
        df_count_dict[table_name] = df_count
        df.to_sql(name=table_name, con=postgres_hook.get_sqlalchemy_engine(), if_exists='append', index=False)

    return df_count_dict


#Task to check the record inside Postgrest
def check_row_counts(**kwargs):
    table_row_counts = {}
    for table_name in Table_Names:
        row_count = postgres_hook.get_first(f"SELECT COUNT(1) FROM {table_name}")[0]
        table_row_counts[table_name] = row_count
        
    df_count_dict = kwargs['ti'].xcom_pull(task_ids='Transform_data_and_insert')
    
    comparison_results = {}
    for table_name, df_count in df_count_dict.items():
        postgres_count = table_row_counts.get(table_name, 0)
        comparison_results[table_name] = {
            'DataFrame Count': df_count,
            'Postgres Count': postgres_count,
            'Match': df_count == postgres_count
        }

    for key, value in comparison_results.items():
        print(f"Key: {key}, Value: {value}")
        print()

# Task to show DAG info
def Dag_info(**kwargs):
    # Get the execution date and end date of the current dag run
    execution_date = kwargs['execution_date']
    end_date = datetime.now()
    # Get all task instances for the current dag run
    ti = kwargs['ti']
    durations = {}
    for task_instance in ti.get_dagrun().get_task_instances():
        if task_instance.duration is not None:  # Exclude tasks with None duration
            durations[task_instance.task_id] = task_instance.duration

    longest_task = max(durations, key=durations.get)
    longest_duration = durations[longest_task]
    shortest_task = min(durations, key=durations.get)
    shortest_duration = durations[shortest_task]

    print(f"DAG Start Time: {execution_date}")
    print(f"DAG End Time: {end_date}")
    for key,value in durations.items():
        print(f"Task {key} executed with duration: {value}")
    print(f"The task with the longest runtime is: {longest_task} with duration: {longest_duration}")
    print(f"The task with the shortest runtime is: {shortest_task} with duration: {shortest_duration}")


# Set task dependencies
file_sensor >> PostgresOperator(
    task_id='create_table',
    sql=sql_create_table,
    postgres_conn_id='Postgress_1',  
    autocommit=True,
    dag=dag,
) >> PythonOperator(
    task_id='Transform_data_and_insert',
    python_callable=Transform_data_and_insert,
    dag=dag,
)>> PythonOperator(
    task_id='check_rows',
    python_callable=check_row_counts,
    provide_context=True,
    dag=dag,
)>> PythonOperator(
    task_id='Dag_infomation',
    python_callable=Dag_info,
    dag=dag,)
