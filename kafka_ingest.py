#!/usr/bin/python
import threading, logging, time
import multiprocessing
import requests
from hdfs import InsecureClient
from kafka import KafkaConsumer, KafkaProducer
#import pyarrow as pa
from subprocess import Popen, PIPE
class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
    def stop(self):
        self.stop_event.set()
    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        while not self.stop_event.is_set():
            response = requests.get("http://api.metro.net/agencies/lametro-rail/vehicles/")
            producer.send('testTopic', b"successfully received")
            producer.send('testTopic', response.content)
            time.sleep(5)
        producer.close()
class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
    def stop(self):
        self.stop_event.set()
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['testTopic'])
        print ("in consumer")
        #client = InsecureClient('http://35.227.182.20:50070', user='koul9puneet@gmail.com')
        #hdfs = pa.HdfsClient('http://35.227.182.20:50070', 20500, 'koul9puneet@gmail.com')
        #print client
        i=0
        while not self.stop_event.is_set():
            for message in consumer:
                i+=1
                print message
                #cat = Popen(["hdfs", "dfs", "-cat", "./sample.txt"], stdout=PIPE)
                #print "cat is ",cat
                #put = Popen(["hdfs", "dfs", "-put", "-", "./modifiedfile.txt"], stdin=cat.stdout)
                #file="location"+str(i)+".txt"
                #file_name="usr/local/Cellar/hadoop/hdfs/locations/"+file
                put = Popen(["hdfs", "dfs", "-put", "-", "usr/local/Cellar/hadoop/hdfs/locations/locDu.txt"], stdin=PIPE)
                put.stdin.write(str(message))
                print put.communicate()
                #hdfs.delete(path)
                #path = '/tmp/test-data-file-1'
                #with hdfs.open(path, 'wb') as f:
                    #f.write(data)
                    #print "writinggggggg------------------"
                ######
                # First, we delete any existing `models/` folder on HDFS.
                #client.delete('models', recursive=True)
                # We can now upload the data, first as CSV.
                #with client.write('models/1.csv', encoding='utf-8') as writer:
                #  for item in message.items():
                #    writer.write(u'%s,%s\n' % item)
                #####
                if self.stop_event.is_set():
                    break
        consumer.close()
def main():
    print "heree"
    tasks = [
        Producer(),
        Consumer()
    ]
    for t in tasks:
        t.start() #Publish and subscribe
    time.sleep(6)
    for task in tasks:
        task.stop() # Set stop event 
    for task in tasks:
        task.join() #Waits till calling thread completes its task 
if __name__ == "__main__":
        logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
        main()
