from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


from datacleaner import data_cleaner, data_downloader, data_machine_learning, data_email_pr,data_email_ml,data_converttoparquet
#from pysql import save_data

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 12, 9),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('store_dag',default_args=default_args,schedule_interval='@daily', template_searchpath=['/home/p/airflow/sql_files'], catchup=True) as dag:
    download = PythonOperator(task_id='data_download', python_callable=data_downloader)

    check_file =BashOperator(task_id='check_file_exists', bash_command='shasum /home/p/airflow/input_raw/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
   

    
    clean_data = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)

    
    mac_learn = PythonOperator(task_id='Train_Save_Model', python_callable=data_machine_learning)
    convert_parquet  = PythonOperator(task_id='Convert_to_Parquet', python_callable=data_converttoparquet)
    
    email_parqet = PythonOperator(task_id='Send_Email_parquet', python_callable=data_email_pr)
    
    email_ml = PythonOperator(task_id='Send_Email_ML_Model', python_callable=data_email_ml)
    
    #t7=BashOperator(task_id='Open_App', bash_command='streamlit run /home/p/airflow/dags/MLapp-Final.py & disown', retries=2, retry_delay=timedelta(seconds=15))
    
    

    
    download >> check_file >> clean_data  >> mac_learn  >> email_ml #>> t7
    clean_data >> convert_parquet >> email_parqet
    #t7
    
