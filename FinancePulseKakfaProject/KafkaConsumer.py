#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem


# In[ ]:


consumer = KafkaConsumer(
    'stock-market-project-demo',
     bootstrap_servers=[':9092'], #add your IP here
     value_deserializer=lambda x: loads(x.decode('utf-8')))


# In[ ]:


for c in consumer:
    print(c.value)


# In[ ]:


s3 = S3FileSystem()


# In[ ]:


for count, i in enumerate(consumer):
    with s3.open("s3://".format(count), 'w') as file: #enter your s3 bucket name
        json.dump(i.value, file) 

