
from kafka import KafkaProducer
import requests
import time
import json
import datetime


def Producer():
	while True:
		producer = KafkaProducer(bootstrap_servers='localhost:9092')
		response=requests.get("http://api.metro.net/agencies/lametro-rail/vehicles/")
		jsonResult=json.loads(response.content)
		gisEntries=jsonResult['items']
		for entry in gisEntries:
			busID=str(entry.get('id'))
			busLong=str(entry.get('longitude'))
			busLat=str(entry.get('latitude'))
			busRoute=str(entry.get('route_id'))
			timeStamp=str(datetime.datetime.now())
			logEntry=timeStamp +'/'+ busID + '/'+busLong+'/'+busLat+'/'+busRoute
			print logEntry
			producer.send('test',logEntry)


if __name__ == '__main__':
	Producer()