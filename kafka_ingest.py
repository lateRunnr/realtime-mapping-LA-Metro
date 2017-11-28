import threading, logging, time
import multiprocessing
import requests
from kafka import KafkaConsumer, KafkaProducer
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
            producer.send('test', b"success")
            producer.send('test', response.content)
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
        consumer.subscribe(['test'])
        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break
        consumer.close()
def main():
    tasks = [
        Producer(),
        Consumer()
    ]
    for t in tasks:
        t.start() #Publish and subscribe
    time.sleep(60)
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
