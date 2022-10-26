import sys
import tweepy
from tweepy import Stream
# from tweepy.streaming import StreamListener
# from tweepy import OAuthHandler
import socket
import json

consumer_key = 'PVl4xGvxOoYL6On2CADkxio56'
consumer_secret = 'LS1Evwz61lnDWn4g6WkI5hSkvX3nHGtLoqprGMUlXL5eP8av3q'
access_token = '1438182019964493826-WswSaq8SN3s3JKoE0WtD6VjwqFOV3d'
access_token_secret= '44zg5SOdTfAkA9jK8OVZXzEQZu0QQ6gJkJzmGNg2e7EiM'


class TweetsListener(Stream):
    # tweet object listens for the tweets
    def __init__(self, csocket):
        self.client_socket = csocket
        self.tweetsCounter = 0
        super().__init__(consumer_key, 
                        consumer_secret,
                        access_token,
                        access_token_secret)

    def on_data(self, data):
        try:
            msg = json.loads(data)
            self.tweetsCounter += 1
            print("new message  #" + str(self.tweetsCounter))
            if self.tweetsCounter >= 10:
                print("twitter thread is ended successfully------1 .........")
                # sys.exit(0)
                self.disconnect()
            # if tweet is longer than 140 characters
            if "extended_tweet" in msg:
                # add at the end of each tweet "t_end"
                self.client_socket\
                    .send(str(msg['extended_tweet']['full_text']+"t_end")
                          .encode('utf-8'))
                print(msg['extended_tweet']['full_text'])
            else:
                # add at the end of each tweet "t_end"
                self.client_socket\
                    .send(str(msg['text']+"t_end")
                          .encode('utf-8'))
                print(msg['text'])

            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True

    def on_disconnect(self):
        print("twitter thread is ended successfully-------2.......")
        sys.exit(0)


def sendData(c_socket, keyword):
    print('start sending data from Twitter to socket')
    # authentication based on the credentials
    # auth = OAuthHandler(consumer_key, consumer_secret)
    # auth.set_access_token(access_token, access_secret)
    # start sending data from the Streaming API
    twitter_stream = TweetsListener(c_socket)
    twitter_stream.filter(track=keyword, languages=["en"])

def connect_to_twitter(keyword:list, host="0.0.0.0", port=5555):
    s = socket.socket()
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword=keyword)



if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword=['piano'])
