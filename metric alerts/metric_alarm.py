import pandas as pd
import pandahouse
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import sys
from read_db.CH import Getch
from datetime import date

def anomaly_detector(df, metric, a=3, n=6):
    df['q25']= df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75']= df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    
    df['top_bound'] = df['q75'] + a* df['iqr']
    df['bottom_bound'] = df['q25'] - a* df['iqr']
    
    df['top_bound'] = df['top_bound'].rolling(n, center = True).mean()
    df['bottom_bound'] = df['bottom_bound'].rolling(n, center = True).mean()
    
    
    if df[metric].iloc[-1] > df['top_bound'].iloc[-1] or df[metric].iloc[-1] < df['bottom_bound'].iloc[-1]:
        trigger = 1
    else:
        trigger = 0
        
    return trigger, df

def alarm(chat=None):
    chat_id = chat or 486496014
    bot = telegram.Bot(token='5380848031:AAHgi6NGVfvpEmBSKKZfspedPHVwvEKUF6M')
    
    data = Getch(''' SELECT ts,
                            date,
                            hm,
                            feed_users,
                            likes,
                            views,
                            CTR,
                            messenger_users,
                            messages
                        FROM 
                            (SELECT
                                    toStartOfFifteenMinutes(time) as ts,
                                    toDate(time) as date,
                                    formatDateTime(ts, '%R') as hm,
                                    uniqExact(user_id) as feed_users,
                                    countIf(user_id, action = 'like') as likes,
                                    countIf(user_id, action = 'view') as views,
                                    100 * likes / views as CTR
                            FROM simulator_20220420.feed_actions
                            WHERE ts >= today()-1  and ts < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts) t1
                            JOIN 
                            (SELECT 
                                    toStartOfFifteenMinutes(time) as ts,
                                    toDate(time) as date,
                                    formatDateTime(ts, '%R') as hm,
                                    uniqExact(user_id) as messenger_users,
                                    count(user_id) as messages
                            FROM simulator_20220420.message_actions
                            WHERE ts >= today()-1 and ts < toStartOfFifteenMinutes(now())
                            GROUP BY ts, date, hm
                            ORDER BY ts) t2
                            ON t1.ts = t2.ts''').df
      
    metrics = ['feed_users', 'likes', 'views', 'CTR', 'messenger_users', 'messages']
    for metric in metrics:
                
        df = data[['ts', 'date', 'hm', metric]].copy()
        trigger, df = anomaly_detector(df, metric)
        df['date'] = pd.to_datetime(df['date']).dt.date
                
        if trigger == 1 or True:
            text = '''Метрика {metric}. за срез ({d1} : {d2})\
                    \nТекущее значение {val:.2f}. Отклонение более {val_diff:.2%}'''\
                    .format(metric = metric,
                            d1 = df['date'].iloc[0],
                            d2 = df['date'].iloc[-1],
                            val = df[metric].iloc[-1],
                            val_diff = abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2])))
            sns.set()
            plt.figure(figsize=(12,8))
            plt.tight_layout()
            p = sns.lineplot(data = df, x='ts', y = metric, label = metric)
            p = sns.lineplot(data = df, x='ts', y = 'top_bound', label = 'top_bound')
            p = sns.lineplot(data = df, x='ts', y = 'bottom_bound', label = 'bottom_bound')
            
            p.set_title('{metric} chart'.format(metric = metric))
            p.set_xlabel('time', fontsize = 10)
            plt.xticks(rotation = 45)
            p.set_ylabel(metric, fontsize = 10)
            p.set(ylim =(0, None))
            
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{metric}.png'.format(metric = metric)
            plt.close()
            
            bot.sendMessage(chat_id = chat_id, text = text)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)
            
            
        
    return

try:
    alarm()
except Exception as e:
    print(e)
