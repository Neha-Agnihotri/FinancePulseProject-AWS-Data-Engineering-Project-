#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install kafka-python


# In[ ]:


import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
from json import dumps
import json


# In[ ]:


producer = KafkaProducer(bootstrap_servers=[' :9092'], #enter your IP
                        value_serializer=lambda x:
                        dumps(x).encode('utf-8'))


# In[ ]:


producer.send('stock-market-project-demo', value={'profit':'yes'})


# In[ ]:


df = pd.read_csv(r"C:\Users\Neha Agnihotri\Downloads\archive.csv")


# In[ ]:


df.head()


# In[ ]:


while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('stock-market-project-demo', value=dict_stock)
    sleep(1)


# In[ ]:


producer.flush() #clear data from kafka server

