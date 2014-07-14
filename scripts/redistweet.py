# redis
# http://ec2-54-187-166-118.us-west-2.compute.amazonaws.com:8888/opscenter/index.html
# ssh -i insightdatastax.pem ubuntu@ec2-54-187-166-118.us-west-2.compute.amazonaws.com
# src/redis-cli
# keys
# ec2
# ssh -i insightxiaohu.pem ubuntu@ec2-54-183-120-89.us-west-1.compute.amazonaws.com
# scp -i insightxiaohu.pem *.py ubuntu@ec2-54-183-120-89.us-west-1.compute.amazonaws.com:~
# sudo apt-get install python-dev
# pip install twitter blist hiredis redis
# nohup python redistweet.py > tweet.out 2> tweet.err < /dev/null &

# * * * * * /usr/bin/flock -n /tmp/redistweet.lockfile python /home/ubuntu/xiaohu/redistweet.py > tweet.out 2> tweet.err < /dev/null & --minutely

import sys, time, string
from datetime import datetime
import json

from twitter import *
import redis

def isValidTweet(tweet):
	if not tweet or 'delete' in tweet or 'limit' in tweet or 'warning' in tweet:
		return False
	return 'created_at' in tweet and tweet['created_at'] \
		and 'text' in tweet and tweet['text'] \
		and 'lang' in tweet and tweet['lang'] == 'en'

def formatdate(twittertime):
	# from: Fri Jun 13 02:06:38 +0000 2014
	# to: 2013-01-01 00:05+0000
	t = time.strptime(twittertime, '%a %b %d %H:%M:%S +0000 %Y')
	dt = datetime.fromtimestamp(time.mktime(t))
	return dt.strftime('%Y-%m-%d %H:%M') + '+0000'

def tweetrecord(tweet):
	return ','.join([str(tweet['coordinates']['coordinates'][0]), str(tweet['coordinates']['coordinates'][1]), tweet['created_at'], \
		filter(lambda x: x in string.printable, tweet['text'].replace('\n', ' '))])

# TODO: remove @, #, http://
# Redis

if __name__ == "__main__":
	from twitter import OAuth
	AUTH = OAuth("2586499028-HcdL7zT3WBoYEPVhCIBTpGQAFQ7SwORVa0sp82e",
		"T3qOIKt13Hvak7anstxKbFKOULCt2EvyPogX9ALl3A6qU",
		"8dYpCyKqxFfTH79FWgFtgWitD",
		"q9y2BGOF2mGILGGGIc5epZFFItqOW4XRrmo20nLEEM1VjmtUGb")

	stream = TwitterStream(auth = AUTH)
	waittime = 10
	starttime = time.time()

	REDIS_SERVER = 'ec2-54-187-166-118.us-west-2.compute.amazonaws.com'
	r = redis.StrictRedis(host=REDIS_SERVER, port=6379, db=0)
	while True:
		try:
			time.sleep(waittime)
			tweet_iter = stream.statuses.sample()
			for tweet in tweet_iter:
				if isValidTweet(tweet):
					try:
						hashtags = [x['text'].encode("utf-8") for x in tweet['entities']['hashtags']]
						if len(hashtags) > 0:
							r.rpush('tweets:tweets', json.dumps({'tweet_id': tweet['id_str'], 'text' : tweet['text'].encode("utf-8"), "hashtags" : hashtags}))
					except Exception, e:
						print >> sys.stderr, e
						continue
		except Exception, e:
			print >> sys.stderr, e
			waittime = min(2 * waittime, 360)
			continue








