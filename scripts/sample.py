from cassandra.cluster import Cluster
import json

if __name__ == "__main__":
	DBSERVER = "ec2-54-187-166-118.us-west-2.compute.amazonaws.com"
	cluster = Cluster([DBSERVER])
	session = cluster.connect()
	# session.execute("CREATE KEYSPACE IF NOT EXISTS TweetsXiaohu WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };")
	session.execute("USE TweetsXiaohu")
	tweets = session.execute('SELECT tweet_id, text, hashtags FROM Tweet')
	# with open('sample.csv','wb') as fout:
	# 	csvwriter = UnicodeWriter(fout, quoting=csv.QUOTE_ALL)
	# 	for tweet in tweets:
	# 		print tweet.text, tweet.hashtags
	# 		csvwriter.writerow([tweet.text] + [hashtag for hashtag in tweet.hashtags])
	with open("sample.txt", "wb") as fout:
		for tweet in tweets:
			print tweet.tweet_id, tweet.text, tweet.hashtags
			# fout.write(tweet.text.encode('utf-8') + "\t" + ",".join([x.encode('utf-8') for x in tweet.hashtags]))
			fout.write(json.dumps({"tweet_id" : tweet.tweet_id, "text" : tweet.text, "hashtags" : ','.join(tweet.hashtags)}) + "\n")





