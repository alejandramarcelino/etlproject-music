from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
	'owner': 'airflow',
	'start_date': datetime(2023, 5, 1),
	'retries' : 2,
}

dag = DAG(
	'etl_pipeline_v8',
	default_args=default_args,
	schedule="0 18 * * *",
	catchup=False
)

#use BashOperator instead of PythonOperator to keep projects separate
#and avoid incompatible versions of libraries
#would alternatively have to include or import code into DAG file

# if you get errors from using python3.10, I recommend using the absolute path for that as well

extract_urls_task = BashOperator(
	task_id='extract_concert_urls',
	bash_command='python3.10 <absolute path of getURLS.py>',
	dag=dag,
)
 
extract_concerts_task = BashOperator(
	task_id='extract_concerts_data',
	bash_command='python3.10 <absolute path of concerts_data.py>',
	dag=dag,
)

clean_concerts_task = BashOperator(
	task_id='clean_concerts_data',
	bash_command='python3.10 <absolute path of cleaning_concerts.py>',
	dag=dag,
)

gett_spotify_task = BashOperator(
	task_id='getting_spotify_info',
	bash_command='python3.10 <absolute path of spotify_data.py>',
	dag=dag,
)

data_dict_task = BashOperator(
	task_id='creating_data_dict',
	bash_command='python3.10 <absolute path of data_dictionary.py>',
	dag=dag,
)


extract_urls_task >> extract_concerts_task >> clean_concerts_task # >> upload to database
get_spotify_task >> data_dict_task #>> upload to database

#upload to database >> scripts related to database 


