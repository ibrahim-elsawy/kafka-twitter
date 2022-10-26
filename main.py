from confluent_kafka import Consumer
from multiprocessing import Process

from producer import producer

# ANSI colors
c = (
	"\033[0m ",   # End of color ---> 0
	"\033[36m ",  # Cyan        ---> 1
	"\033[91m ",  # Red        ---> 2
	"\033[35m ",  # Magenta  ---> 3
	"\033[32m ",  # Green    ---> 4
	"\033[33m "   # Yellow    ---> 5
	"\033[34m "   # Blue       ---> 6
)

if __name__ == '__main__': 
	print("starting......")
	c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'}) 
	print('Kafka Consumer has been initiated...')

	c.subscribe(['main3'])

	procs = []

	while True:
		msg=c.poll(1.0) #timeout
		if msg is None:
			continue
		if msg.error():
			print('Error: {}'.format(msg.error()))
			continue
		data=msg.value().decode('utf-8')
		keyword = [ data.split("#*#")[0] ]
		dataId = data.split("#*#")[1]
		proc = Process(target = producer, args=("main4", keyword, dataId),)
		print(data)
		procs.append(proc)
		proc.start()
		# print(proc.pid)
		# proc.join()
	c.close()