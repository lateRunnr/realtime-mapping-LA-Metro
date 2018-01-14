
from kafka import KafkaConsumer
import os
import datetime

batch_count=1
timeStamp=""
tmpfile_path=""

#Function for cleaning timestamp for file naming conventions
def clean_time_stamp(timeStamp):
	breakTime=timeStamp.split(" ")
	cleanTime=breakTime[0]+"-"+breakTime[1]
	return cleanTime


# Function to transfer date from temporary file to HDFS
def copy_to_HDFS(topic,output_dir):
	## Code to put data in hdfs files
	global batch_count, timeStamp,tmpfile_path
	hadoop_dir_loc="{}/{}".format(output_dir,topic)
	hadoop_path = hadoop_dir_loc + "/{}_{}.txt".format(timestamp, batch_count)
	os.system("/usr/bin/hdfs dfs -mkdir {}".format(hadoop_dir_loc))
	os.system("/usr/bin/hdfs dfs -put -f {} {}".format(tmpfile_path,hadoop_path))
	os.remove(tmpfile_path)
	batch_count+=1
	tmpfile_path="/Users/puneetkoul/Desktop/LA_Metro/real-time-tracking-sytem/{}-{}-{}.txt".format(topic,timeStamp,batch_count)
	tmpfile_path=open(tmpfile_path,"w")


# Function for storing producer data into batch layer (HDFS), with intermediate temp file
def consume(topic,output_dir):
	global timeStamp, tmpfile_path,batch_count
	timeStamp=str(datetime.datetime.now())
	timeStamp=str(clean_time_stamp(timeStamp))
	tmpfile_path="/Users/puneetkoul/Desktop/LA_Metro/real-time-tracking-sytem/{}-{}-{}.txt".format(topic,timeStamp,batch_count)
	tempfile=open(tmpfile_path,"w")
	print tmpfile_path
	consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest',consumer_timeout_ms=1000)
	consumer.subscribe(['test'])
	for message in consumer:
		tempfile.write(message.value + "\n")
		if tempfile.tell() > 10000000:
			pass
			copy_to_HDFS(topiv,output_dir)
	consumer.close()



if __name__ == '__main__':
	output_dir="/user/gisdata"
	topic='test'
	consume(topic,output_dir)