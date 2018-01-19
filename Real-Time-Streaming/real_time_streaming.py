# -*- coding: UTF-8 -*-
import sys,pprint
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    print "here"
    sc=SparkContext(appName="MapMyMetro")
    ssc=StreamingContext(sc,2)
    #ssc.checkpoint()
    #kvs=KafkaUtils.createStream(ssc,broker,"raw-streaming-consumer",{topic:1})
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    #kvs=KafkaUtils.createDirectStream(ssc, [topic], {broker.list:'localhost:9092'})
    lines=kvs.map(lambda x : x[1])
    lines.pprint()
    ssc.start()
    ssc.awaitTermination()
