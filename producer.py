from threading import Thread
from model.sentiment_analysis import do_sent_analysis

from model.twitter_connection import connect_to_twitter


# def createTopic (topicName):
# 	NewTopic(topicName, 1, 1)

def producer(topicName, keyword:list, dataId):
	thread = Thread(target = connect_to_twitter, args = (keyword, ))
	thread.start()
	do_sent_analysis(topicName, dataId)
	thread.join()