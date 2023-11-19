import requests
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# откуда берём данные
TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    ### берём данные
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)
get_data()

def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_10_zones = top_data_df['domain'].apply(lambda x: x.split('.')[-1]).value_counts().head(10)
    print(top_10_zones)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_10_zones.to_csv(index=False, header=False))

def get_longest_domain_name():
    longest_name_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len = longest_name_df['domain'].str.len().max()
    longest_name = longest_name_df[longest_name_df['domain'].str.len() == max_len]
    print("Longest domain name: ", longest_name['domain'])

    with open('longest_name.csv', 'w') as f:
        f.write(longest_name.to_csv(index=False, header=False))

def airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    air_rank = df.query('domain == "airflow.com"')
    print('airflow.com rank is ', air_rank['rank'])

    with open('air_rank.csv', 'w') as f:
        f.write(air_rank['rank'].to_csv(index=False, header=False))

def print_data(ds):
    with open('top_10_zones.csv', 'r') as f:
        top_10_zones = f.read()
    with open('longest_name.csv', 'r') as f:
        longest_name = f.read()
    date = ds
    with open('air_rank.csv', 'r') as f:
        air_rank = f.read()

    print(f'Top zones by size for date {date}')
    print(top_10_zones)

    print(f'Longest domain for date {date}')
    print(longest_name)

    print(f'Airflow.com rank for date {date}')
    print(air_rank)

default_args = {
    'owner': 'g-gerasimchuk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now()
}
schedule_interval = '0 12 * * *'

dag = DAG('g-gerasimchuk', default_args=default_args, schedule_interval=schedule_interval)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='get_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t2_long = PythonOperator(task_id='get_longest_name',
                        python_callable=get_longest_domain_name,
                        dag=dag)

t2_air = PythonOperator(task_id='get_airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2, t2_long, t2_air] >> t3



#t1.set_downstream(t2)
#t1.set_downstream(t2_com)
#t2.set_downstream(t3)
#t2_com.set_downstream(t3)