#!/usr/bin/python

import json
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream, TwitterError
import re
import time,string
import requests
import datetime

start_time = time.time()
#hour=0
#filename = 'tweets_%02d' % hour
filename = 'tweets_'+datetime.datetime.utcnow().isoformat()
file = open(filename,'w')

try:
    with open("credentials.json") as data_file:
        credentials = json.load(data_file)
except:
    print ("Cannot load credentials.json")

oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'],credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
twitter_stream = TwitterStream(auth=oauth)
iterator = twitter_stream.statuses.sample()

start_time = time.time()
flush_time = start_time

for tweet in iterator:
    try:
       #print(json.dumps(tweet)+"\n")
       file.write(json.dumps(tweet)+"\n")

    except TwitterError as e:
	print(e)
        time.sleep(20)
        pass
    except requests.ConnectionError as e:
        time.sleep(20)
        print(e)
        pass
    except TwitterHTTPError as e:
        print(e)
        time.sleep(300)
        pass
    except timeout:
    	print('socket timed out')
        file.close()
        break
    except:
        pass

    elapsed_time = time.time() - flush_time
    #flush every minute
    if elapsed_time > 60:
        file.flush()
        flush_time = time.time()

    elapsed_time = time.time() - start_time
    #create new file every hour
    if elapsed_time > 60*60:
        #hour = hour + 1
        file.close()
	filename = 'tweets_'+datetime.datetime.utcnow().isoformat()
        #filename = 'tweets_%02d' % hour
        file = open(filename,'w')
        start_time = time.time()
