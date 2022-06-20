from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandahouse as ph
import pandas as pd




default_args = {
    'owner': 'a.koshkin',
    'depends_on_past': False, 
    'retries': 2, 
    'retry_delay': timedelta(minutes=5),  
    'start_date': datetime (2022, 5, 26)
}

schedule_interval = '0 12 * * *'

def query_to_df(query):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database': 'simulator',
                  'user': 'student',
                  'password': 'dpo_python_2020'
}

    df = ph.read_clickhouse(query, connection=connection)
    return df


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_table_to_ch():
    
    @task
    def extract_from_feed():
        q1 = """SELECT toDate(time) as event_date,
                          user_id as users,
                          countIf(user_id, action = 'like') as likes,
                          countIf(user_id, action = 'view') as views,
                          gender,
                          os,
                          multiIf(age < 10, '-10',
                                  age >= 10 and age < 20, '10-19',
                                  age >= 20 and age < 30, '20-29',
                                  age >= 30 and age < 40, '30-39',
                                  age >= 40 and age < 50, '40-49',
                                  age >= 50 and age < 60, '50-59',
                                  '60+') as age
              FROM simulator_20220420.feed_actions
              WHERE event_date = yesterday()
              GROUP BY event_date, users, gender, os, age
    """
        df_feed = query_to_df(q1)
        return df_feed

    @task
    def extract_from_msg():
        q2 = """SELECT  event_date,
            users,
            messages_sent,
            messages_received,
            users_sent,
            users_received,
            gender,
            os,
            multiIf(age < 10, '-10',
                                  age >= 10 and age < 20, '10-19',
                                  age >= 20 and age < 30, '20-29',
                                  age >= 30 and age < 40, '30-39',
                                  age >= 40 and age < 50, '40-49',
                                  age >= 50 and age < 60, '50-59',
                                  '60+') as age

            FROM
                (SELECT toDate(time) as event_date,
                       user_id as users,
                       count(reciever_id) as messages_sent,
                       uniq(reciever_id) as users_sent,
                       gender,
                       os,
                       age
                FROM simulator_20220420.message_actions
                WHERE event_date = yesterday()
                GROUP BY event_date, users, gender, os, age) t1
                FULL OUTER JOIN
                (SELECT toDate(time) as event_date,
                       reciever_id as users,
                       count(user_id) as messages_received,
                       uniq(user_id) as users_received,
                       gender,
                       os,
                       age
                FROM simulator_20220420.message_actions
                WHERE event_date = yesterday()
                GROUP BY event_date, users, gender, os, age) t2
            USING users
    """
        df_msg = query_to_df(q2)
        return df_msg

    @task
    def merge_table(df_feed, df_msg):
        df = pd.merge(df_feed, df_msg)
        return df

    @task
    def split_by_gender(df):
        gender_split_df = df.groupby(['event_date', 'gender']).sum().reset_index()
        gender_split_df['gender'] = gender_split_df['gender'].apply(lambda x: 'male' if x == 1 else 'female')
        gender_split_df['metric'] = 'gender: '+ gender_split_df['gender']
        return gender_split_df

    @task
    def split_by_os(df):
        os_split_df = df.groupby(['event_date', 'os']).sum().reset_index()
        os_split_df['metric'] = 'os: '+ os_split_df['os']
        return os_split_df

    @task
    def split_by_age(df):
        age_split_df = df.groupby(['event_date', 'age']).sum().reset_index()
        age_split_df['metric'] = 'age:' + age_split_df['age']
        return age_split_df

    @task
    def concat_splits(gender_split_df, os_split_df, age_split_df):
        to_clickhouse_df = pd.concat([gender_split_df, os_split_df, age_split_df]).reset_index(drop=True)
        to_clickhouse_df = to_clickhouse_df.drop(columns = ['gender', 'os', 'age'])

        #to_clickhouse_df = pd.DataFrame()
        for col in to_clickhouse_df.columns:
            if col == 'event_date':
                to_clickhouse_df[col] = to_clickhouse_df[col].apply(lambda x: datetime.isoformat(x))
            elif col == 'metric':
                to_clickhouse_df[col] = to_clickhouse_df[col]
            else:
                to_clickhouse_df[col] = to_clickhouse_df[col].astype(int)

        return to_clickhouse_df


    @task
    def upload_to_ch(to_clickhouse_df):
        connection_2 = {'host':'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw',
                      'password':'656e2b0c9c'}
        
        q3 = '''CREATE or replace TABLE test.af_table_koshkina(
            event_date DateTime,
            users INTEGER,
            likes INTEGER,
            views INTEGER,
            messages_sent INTEGER,
            messages_received INTEGER,
            users_sent INTEGER,
            users_received INTEGER,
            metric TEXT
            )
            ENGINE = MergeTree 
            ORDER BY (event_date)'''
        ph.execute(q3, connection=connection_2)
        ph.to_clickhouse(to_clickhouse_df, 'af_table_koshkina', index=False, connection=connection_2)


           
    #extract
    df_feed = extract_from_feed()
    df_msg = extract_from_msg()
    #transform
    df = merge_table(df_feed, df_msg)
    gender_split_df = split_by_gender(df)
    os_split_df = split_by_os(df)
    age_split_df = split_by_age(df)
    to_clickhouse_df = concat_splits(gender_split_df, os_split_df, age_split_df)
    #load
    upload_to_ch(to_clickhouse_df)
    
dag_table_to_ch = dag_table_to_ch()