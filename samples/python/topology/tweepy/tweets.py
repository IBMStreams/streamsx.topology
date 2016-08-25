# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016
import queue
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

import mykeys

def text(item):
   """
    Get the text from the tweet
   """
   if 'text' in item:
       return item['text']
   return None

#
# Event based source.
# This class is a tweepy StreamListener
# which is an event handler. Each tweet
# results in a call to on_data. Here
# we add the tweet to a Queue as
# a dictionary object created from the JSON.
#
# An instance of this class can be passed
# to the source as it's callable returns
# a iterable whose iterator will be an
# iterator against the queue. Iterating
# over the queue removes the tweet so
# that is will be placed on the source stream
# If the queue is empty the iterator blocks until
# until a new tweet arrives.
# 
#
class tweets(StreamListener):
   def __init__(self, terms):
       self.terms = terms

   def on_data(self, data):
       self.items.put(json.loads(data))
       return True
   def on_error(self, status):
       if status == 420:
            return False
       return False

   def __call__(self):
       self.items = queue.Queue()
       auth = OAuthHandler(mykeys.ckey, mykeys.csecret)
       auth.set_access_token(mykeys.atoken, mykeys.asecret)
       self.stream = Stream(auth, self)
       self.stream.filter(track=self.terms, async=True)
       return self

   def __iter__(self):
     return iter(self.items.get, None)
