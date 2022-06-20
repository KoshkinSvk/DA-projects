import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
import os
import datetime as dt

sns.set()

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220520'
    }

q1 = ''' SELECT toDate(time) as date,
                uniqExact(user_id) as m_users,
                count(user_id) as messages,
                messages / m_users as m_per_user
         FROM simulator_20220520.message_actions
         WHERE toDate(time) between today()-7 and today()-1
         GROUP BY date
         ORDER BY date
'''
df1 = pandahouse.read_clickhouse(q1, connection=connection)

q2 = '''SELECT uniqExact(user_id) as new_users,
               date
        FROM 
            (SELECT user_id,
                    MIN(prev_time) as date
             FROM 
                    (SELECT user_id,
                            MIN(toDate(time)) as prev_time
                     FROM simulator_20220520.feed_actions
                     WHERE toDate(time) between today()-30 and today()-1
                     GROUP BY user_id
                     ORDER BY user_id
                UNION ALL
                     SELECT user_id,
                            MIN(toDate(time)) as prev_time
                     FROM simulator_20220520.message_actions
                     WHERE toDate(time) between today()-30 and today()-1
                     GROUP BY user_id
                     ORDER BY user_id) t1
            GROUP BY user_id) t2
WHERE date between today() -7 and today()-1
GROUP BY date

'''
df2 = pandahouse.read_clickhouse(q2, connection = connection)

df1['date'] = pd.to_datetime(df1['date']).dt.date
df2['date'] = pd.to_datetime(df2['date']).dt.date

text = f'Метрики месенджера за {df1.date[6]}:\
               \n  DAU: {df1.m_users[6]};\
               \n  Messages: {df1.messages[6]};\
               \n  Messages per user:   {df1.m_per_user[6]:.2f}\
               \nNew app users: {df2.new_users[6]}'

def test_report(chat=-799682816):
    chat_id = chat or 486496014
    bot = telegram.Bot(token='5380848031:AAHgi6NGVfvpEmBSKKZfspedPHVwvEKUF6M')
    text = f'Метрики месенджера за {df1.date[6]}:\
               \n  DAU: {df1.m_users[6]};\
               \n  Messages per day: {df1.messages[6]};\
               \n  Message per user:   {df1.m_per_user[6]:.2f}\
               \nNew app users: {df2.new_users[6]}'
    bot.sendMessage(chat_id, text = text)
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df1, x= 'date', y = 'm_users')
    plt.title('DAU Messenger chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'DAU messenger.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df1, x= 'date', y = 'm_per_user')
    plt.title('Message per user chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'Message per user.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    

    
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df2, x= 'date', y = 'new_users')
    plt.title('New users chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'New users.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)  


try:
    test_report()
except Exception as e:
    print(e)
