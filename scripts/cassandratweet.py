# cassandra
# http://ec2-54-187-166-118.us-west-2.compute.amazonaws.com:8888/opscenter/index.html
# ssh -i insightdatastax.pem ubuntu@ec2-54-187-166-118.us-west-2.compute.amazonaws.com
# cqlsh
# > desc keyspaces;
# > use tweetsxiaohu;
# > desc tables;
# > select count(*) from tweet limit 1000000;

# ec2
# ssh -i insightxiaohu.pem ubuntu@ec2-54-183-120-89.us-west-1.compute.amazonaws.com
# scp -i insightxiaohu.pem *.py ubuntu@ec2-54-183-120-89.us-west-1.compute.amazonaws.com:~
# pip install twitter, cassandra-driver, blist
# nohup python cassandratweet.py > tweet.out 2> tweet.err < /dev/null &

# * * * * * /usr/bin/flock -n /tmp/cassandratweet.lockfile python /home/ubuntu/cassandratweet.py > tweet.out 2> tweet.err < /dev/null & --minutely

import sys, time, string
from datetime import datetime
import requests, json

from twitter import *
from cassandra.cluster import Cluster

from config import AUTH, DBSERVER, TWEET_TTL

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

	cluster = Cluster([DBSERVER])
	session = cluster.connect()
	# session.execute("CREATE KEYSPACE IF NOT EXISTS TweetsXiaohu WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
	session.execute("USE TweetsXiaohu")
	# session.execute("DROP TABLE IF EXISTS Tweet")
	session.execute("""
		CREATE TABLE IF NOT EXISTS Tweet (
			tweet_id bigint PRIMARY KEY,
			user_name text,
			text text,
			created_at timestamp,
			hashtags list<text>,
			lang text,
		) WITH comment='tweets from twitter stream'
	""")

	stream = TwitterStream(auth = AUTH)
	waittime = 10
	starttime = time.time()

	while True:
		try:
			time.sleep(waittime)
			tweet_iter = stream.statuses.sample()
			for tweet in tweet_iter:
				if isValidTweet(tweet):
					try:
						# tweet_data.write(tweetrecord(tweet) + '\n')
						# curl -X POST -d 'json={"action":"login","user":2}' http://localhost:8888/hdfs.access.test
						url = 'http://ec2-54-183-120-91.us-west-1.compute.amazonaws.com:8888/hdfs.access.tweet'
						# url = 'http://localhost:8888/hdfs.access.test'
						# payload = {"action":"login", "user" : 190}
						headers = {'content-type': 'application/json'}

						response = requests.post(url, data=json.dumps(tweet), headers=headers)
						# print response

						hashtags = [x['text'].encode("utf-8") for x in tweet['entities']['hashtags']]
						if len(hashtags) > 0:
							cql = """
								INSERT INTO Tweet (tweet_id, user_name, text, created_at, hashtags, lang) 
									VALUES ({tweet_id}, '{user_name}', '{text}', '{created_at}', {hashtags}, '{lang}') USING TTL {TTL}; 
								""".format(tweet_id = tweet['id_str'], user_name = tweet['user']['screen_name'].encode("utf-8").replace("'", "''"), text = tweet['text'].encode("utf-8").replace("'", "''"), \
									created_at = formatdate(tweet['created_at']), hashtags = hashtags, \
									lang = tweet['lang'], TTL = TWEET_TTL)
							# print cql.strip()
							session.execute(cql)
					except Exception, e:
						print >> sys.stderr, e
						print cql.strip()
						continue
		except Exception, e:
			print >> sys.stderr, e
			waittime = min(2 * waittime, 360)
			continue








