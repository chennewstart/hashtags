register 'tf-idf.py' using jython as myfuncs;
-- register 'lib/pig-redis.jar';
register 'lib/elephant-bird-pig-4.6-SNAPSHOT.jar'
register 'lib/elephant-bird-hadoop-compat-4.6-SNAPSHOT.jar'
register 'lib/json-simple-1.1.1.jar'

tweets = LOAD '/tmp/{20140701,20140702,20140703,20140704,20140705,20140706}' USING com.twitter.elephantbird.pig.load.JsonLoader() as (json:map[]);
--tweets = LIMIT tweets 100;

tweets = FOREACH tweets GENERATE (CHARARRAY)$0#'id' AS tweet_id, (CHARARRAY)$0#'text' AS text;
-- dump tweets;
-- describe tweets;

-- CqlStorage
-- tweets = LOAD 'cql://tweetsxiaohu/tweet' USING CqlStorage();
-- tweets = LIMIT tweets 100000;
-- dump tweets;
-- describe tweets;

total = FOREACH (GROUP tweets ALL) GENERATE 'total', COUNT(tweets);
-- dump total;
-- (total,15,764,809)
-- describe total;
-- STORE total INTO 'dummy-filename-is-ignored' USING com.hackdiary.pig.RedisStorer('kv', 'ec2-54-187-166-118.us-west-2.compute.amazonaws.com');
STORE total INTO 'pigoutput-20140706/total' using PigStorage(',');

tweets = FOREACH tweets GENERATE tweet_id, myfuncs.tokenize(text) as words;
-- tweets = FOREACH tweets GENERATE tweet_id, TOKENIZE(text) as words;
-- dump tweets;
-- describe tweets;
tokens = FOREACH tweets GENERATE tweet_id, FLATTEN(words) as token;
-- dump tokens;
-- describe tokens;
tokens_group = GROUP tokens by token;
-- dump tokens_group;
-- describe tokens_group;
df = FOREACH tokens_group GENERATE group as token, COUNT(tokens) as freqency;
df = FILTER df by freqency > 1;
-- dump df;
-- describe df;
-- STORE df INTO 'dummy-filename-is-ignored' USING com.hackdiary.pig.RedisStorer('kv', 'ec2-54-187-166-118.us-west-2.compute.amazonaws.com');
STORE df INTO 'pigoutput-20140706/df' using PigStorage(',');

