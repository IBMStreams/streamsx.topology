# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019
from streamsx.topology.topology import *
import streamsx.topology.context
from streamsx.topology.schema import *
import sys
import tweets

#
# Continually stream tweets that contain
# the terms passed on the command line.
#
# python3 app.py Food GlutenFree
#
#
# Requires tweepy to be installed
#
# pip3 install tweepy
#
# http://www.tweepy.org/
#
# You must create Twitter application authentication tokens
# and set them in the mykeys.py module.
# Note this is only intended as a simple sample,
#

def main():
  terms = sys.argv[1:]
  topo = Topology("TweetsUsingTweepy")

  # Event based source stream
  # Each tuple is a dictionary containing
  # the full tweet (converted from JSON)
  ts = topo.source(tweets.tweets(terms))

  # Publish the full tweets to a topic as JSON
  ts.publish("tweets/full", schema=CommonSchema.Json)
  
  # get the text of the tweet
  ts = ts.map(tweets.text)

  # Publish just the test of tweets to a topic as String
  ts.publish("tweets/text", schema=CommonSchema.String)

  streamsx.topology.context.submit("DISTRIBUTED", topo)

if __name__ == '__main__':
    main()
