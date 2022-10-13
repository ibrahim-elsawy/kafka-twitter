from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

def delivery_report(err, msg):
	""" Called once for each message produced to indicate delivery result.
	Triggered by poll() or flush(). """
	if err is not None:
		print('Message delivery failed: {}'.format(err))
	else:
		print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def createTopic (topicName):
	NewTopic(topicName, 1, 1)

def producer(topicName, id):
	# createTopic(topicName)
	data = "this msg from python producer"+"#*#"+id
	p = Producer({'bootstrap.servers': 'localhost:9092'})
	print('Kafka Producer has been initiated...')
	p.poll(0)
	p.produce(topicName, data.encode('utf-8'), callback=delivery_report)
	p.flush()