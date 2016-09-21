import re

tweets = ["This is an example of a tweet! #twitter #python",
          "We have multiple #tweets, each one will be passed as a #tuple",
          "We can extract the #hashtags from each one. #datastreaming #cool"]

class TweetSource:
    def __call__(self):
        for tweet in tweets:
            yield tweet
                   
class HashTags:
    def __call__(self, tweet):
        return re.findall(r"#(\w+)", tweet) # is a list