import math
import redis

"""
update word count regarding to out-of-vocabulary (OOV), such as "sureeeeeeeeeeeeeee"(sure) and "coz"(because)
see Creutz et al., Morph-based speech recognition and modeling of out-of-vocabulary words across languages, 2007
oov in all forms will have the same count (the sum) after this process

used right after tf-idf.pig and only once
"""

def toint(string):
	'''
	change a string to int, if None return 0
	'''
	return int(string) if string is not None else 0

if __name__ == "__main__":
	'''
	update word count in the redis server with oov.txt
	'''
	REDIS_SERVER = 'ec2-54-187-166-118.us-west-2.compute.amazonaws.com'
	r = redis.StrictRedis(host=REDIS_SERVER, port=6379, db=0)

	with open('pigoutput-20140706/total/part-r-00000') as f:
		total = int(f.readline().split(',')[1])
		print 'd:total', r.set('d:total', total)

	with open('pigoutput-20140706/df/part-r-00000') as f:
		# print r.hmset('chen:token', {'df' : '2', 'vocabulary' : 'chen:token'})
		for line in f:
			token, df = line.split(',')
			print 'token' + ':token', r.hmset(token + ':token', {'df' : int(df), 'vocabulary' : token + ':token'})

	mapping = {}
	with open('oov.txt') as f:
		for line in f:
			oov, word = line.split()
			mapping[oov + ':token'] = word + ':token'
	
	for oov, word in mapping.items():
		newdf = toint(r.hget(oov, 'df')) + toint(r.hget(word, 'df'))
		print word, r.hset(word, 'df', newdf)

	total = float(r.get('d:total'))
	tokens = r.keys('*:token')
	position = 0
	for token in tokens:
		if token not in mapping:
			print token, r.hmset(token, 'idf', math.log(total) / (float(r.hget(token, 'df')) + 1), 'position', position)
			position += 1

	for token in tokens:
		if token in mapping:
			print token, r.hmset(token, r.hgetall(mapping[token]))




