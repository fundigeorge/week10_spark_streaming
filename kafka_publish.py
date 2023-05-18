from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import random
import time
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError
import streamlit as st
#kafka config
kafka_params = {    
"bootstrap.servers":"pkc-lzvrd.us-west4.gcp.confluent.cloud:9092",
"security.protocol":"SASL_SSL",
"sasl.mechanisms":"PLAIN",
"sasl.username":"ZLGCPXOQFPUP7FAR",
"sasl.password":"eRLHg0rYcspB6IrQdHws/99n2FBxZrdvnfC9CsyZq85EblQJgRvlsEB+8gaHXhBI",
"group.id":"kafka_spark_streaming",
}


#create a kafka producer
producer = Producer(kafka_params)
#a topic in kafka
kafka_topic = "transactions"

#a sample data
def gen_transaction():
    transcation = {
        'account_id': random.randint(1, 100),
        'transaction_id': random.randint(1, 1000),
        'amount': round(random.uniform(10.0, 1000.0), 2),
        'timestamp': int(time.time())
        }
    return transcation   

#start/stop datastream publishing to kafka
st.write("Simulating data streaming to kafka on confluent cloud")
col1, col2 = st.columns(2, gap='small')
with col1:
    start_kafka_stream = st.button("START DATA STREAMING..")
with col2:
    stop_kafka_stream = st.button("STOP DATA STREAMING")

#stream data to kafka topic using while
if start_kafka_stream:     
    st.write("start publishing data..")
    number_of_write = 0
    while True:
        data = gen_transaction()
        serialized_data = json.dumps(data).encode("utf-8")
        #publish the data to kafka topic
        producer.produce(kafka_topic, value = serialized_data)
        producer.flush()
        time.sleep(2)
        #stop the publishing of data
        number_of_write += 1
        st.write(number_of_write)
        if stop_kafka_stream:
            st.write('stopped data publish on kafka')
            break
        
st.write("Consuming data stream from kafka")

#consume data from kafka
consumer = Consumer(kafka_params)
consumer.subscribe([kafka_topic])
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f'error:{data.error()}')
        continue
    msg = json.loads(msg.value().decode('utf-8'))
    print(msg)
    print("read msg from kafka")
    


