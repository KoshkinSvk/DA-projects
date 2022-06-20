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
    'database': 'simulator_20220420'
    }

q1 = ''' SELECT toDate(time) AS date,
                COUNT(DISTINCT(user_id)) as DAU
         FROM simulator_20220420.feed_actions
         WHERE toDate(time) between  today() - 8 and today() - 1
         GROUP BY date
         ORDER BY date
'''
df1 = pandahouse.read_clickhouse(q1, connection=connection)

q2 = ''' SELECT countIf(user_id, action = 'like') as likes,
               countIf(action = 'view') as views,
               100 * likes/ views as CTR,
               toDate(time) AS date
        FROM simulator_20220420.feed_actions
        WHERE toDate(time) between  today() - 8 and today() - 1
        GROUP BY date
        ORDER BY date
'''
df2 = pandahouse.read_clickhouse(q2, connection = connection)

df1['date'] = pd.to_datetime(df1['date']).dt.date
df2['date'] = pd.to_datetime(df2['date']).dt.date

def test_report(chat=-799682816):
    chat_id = chat or 486496014
    bot = telegram.Bot(token='5380848031:AAHgi6NGVfvpEmBSKKZfspedPHVwvEKUF6M')
    text = f'Основные мети за {df1.date[6]}:\
            \n DAU:   {df1.DAU[6]:.2f};\
               \n Likes: {df2.likes[6]};\
               \n Views: {df2.views[6]};\
               \n CTR:   {df2.CTR[6]:.2f};'
    bot.sendMessage(chat_id, text = text)
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df1, x= 'date', y = 'DAU')
    plt.title('DAU chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'DAU.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df2, x= 'date', y = 'likes')
    plt.title('Likes chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'likes chart.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df2, x= 'date', y = 'views')
    plt.title('Views chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'views chart.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df2, x= 'date', y = 'CTR')
    plt.title('CTR chart')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'CTR chart.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)  


try:
    test_report()
except Exception as e:
    print(e)
