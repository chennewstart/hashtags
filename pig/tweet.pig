-- insightbingo!
register 'tweet.py' using jython as myfuncs;
-- register 'lib/pig-redis.jar';
register 'lib/elephant-bird-pig-4.6-SNAPSHOT.jar'
register 'lib/elephant-bird-hadoop-compat-4.6-SNAPSHOT.jar'
register 'lib/json-simple-1.1.1.jar'

tweets = LOAD '/tmp/{20140715,20140714,20140713,20140712,20140711,20140710,20140709}' USING com.twitter.elephantbird.pig.load.JsonLoader() as (json:map[]);
-- tweets = LOAD '/tmp/access.log.ip-172-31-4-214-head' USING com.twitter.elephantbird.pig.load.JsonLoader() as (json:map[]);
-- tweets = LIMIT tweets 1000;

tweets = FOREACH tweets GENERATE (CHARARRAY)$0#'id' AS tweet_id, (CHARARRAY)$0#'text' AS text, com.twitter.elephantbird.pig.piggybank.JsonStringToMap($0#'entities') as entities;

-- tweets = DISTINCT tweets;
tweets =
    FOREACH (GROUP tweets BY tweet_id) {
        result = TOP(1, 0, $1);
        GENERATE FLATTEN(result);
    };

tweets = foreach tweets generate tweet_id, text, myfuncs.extracthashtags(entities#'hashtags') as hashtags;
tweets = filter tweets by hashtags is not null;

tweets = foreach tweets generate tweet_id, text as oldtext, myfuncs.cleantweet(text) as tweet, hashtags;
tweets = filter tweets by tweet is not null;

dump tweets;
describe tweets;
STORE tweets INTO 'large/tweets.json' USING JsonStorage();
-- store tweets into 'csvoutput' using PigStorage(',','-schema');
