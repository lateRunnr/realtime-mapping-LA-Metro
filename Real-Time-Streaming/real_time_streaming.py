# -*- coding: UTF-8 -*-
import sys,pprint
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def get_vehicle_info(line):
    routePoint=line[4],(line[2],line[3])
    return routePoint

def flush_vehicle_loc(routeInfo):
    print routeInfo[0]
    for coordinates in routeInfo[1]:
        # Flush coordinates based on route
        # routeID = routInfo[0]
        # vehiclePos = coordinates
        print coordinates
    return routeInfo


if __name__ == "__main__":
    print "here"
    sc=SparkContext(appName="MapMyMetro")
    ssc=StreamingContext(sc,2)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines=kvs.map(lambda x : x[1])
    lines=lines.map(lambda line: line.split("/"))
    trip=lines.map(get_vehicle_info).groupByKey()
    trip=trip.map(flush_vehicle_loc)
    trip.pprint()
    ssc.start()
    ssc.awaitTermination()
