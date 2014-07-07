import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('keys.cfg')

CONSUMER_KEY = config.get('OAuth', 'API key')
CONSUMER_SECRET = config.get('OAuth', 'API secret')
OAUTH_TOKEN = config.get('OAuth', 'Access token')
OAUTH_TOKEN_SECRET = config.get('OAuth', 'Access token secret')

from twitter import OAuth
AUTH = OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET)

DBSERVER = config.get('Cassandra', 'dbserver')
TWEET_TTL = config.get('Cassandra', 'ttl')