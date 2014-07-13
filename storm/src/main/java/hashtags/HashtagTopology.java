package hashtags;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.MemoryMapState;
import hashtags.bolt.ComputeDistance;
import hashtags.bolt.CountAggKeep;
import hashtags.bolt.ExpandList;
import hashtags.bolt.Extractor;
import hashtags.bolt.FakeID;
import hashtags.bolt.FilterLow;
import hashtags.bolt.FirstNAggregator;
import hashtags.bolt.Preprocessor;
import hashtags.bolt.Tokenizer;
import hashtags.bolt.Vectorizer;
import hashtags.bolt.WrapToTweet;
import hashtags.spout.RedisSpout;
import hashtags.spout.TweetSpout;
import hashtags.state.BucketsStateFactory;
import hashtags.state.BucketsStateQuery;
import hashtags.state.BucketsStateUpdate;
import hashtags.state.DFStateQuery;
import hashtags.state.DFStateUpdateQuery;
import hashtags.state.DStateQuery;
import hashtags.state.DStateUpdateQuery;
import hashtags.state.PosStateFactory;
import hashtags.state.PosStateQuery;
import hashtags.state.PosStateUpdateQuery;

/***
 * 
 * @author Xiaohu Chen
 * 
 *         Real-time #hashtag suggestions for tweets
 * 
 *         Two topology running, with one trident topology turning tweets that
 *         have hashtags to TF-IDF vector and then putting them into different
 *         buckets using Location Sensitive Hashing (LSH), and another DRPC
 *         topology receive new tweets that do not have hashtags and make
 *         hashtags suggestions based on the similar tweets in the same buckets
 * 
 */

public class HashtagTopology {

	public static StormTopology buildTopology(LocalDRPC drpc) {

		RedisSpout spout = new RedisSpout();
		TridentTopology topology = new TridentTopology();

		TridentState bucketsDB = topology
				.newStaticState(new BucketsStateFactory());

		Stream stream = topology
				.newStream("spout1", spout)
				.parallelismHint(12)
				.shuffle()
				// .each(new Fields("tweet_id", "text", "hashtags"), new
				// Debug());
				.each(new Fields("text"), new Preprocessor(),
						new Fields("cleantext"))
				.parallelismHint(12)
				// .project(new Fields("tweet_id", "text", "hashtags"))
				.each(new Fields("cleantext"), new Tokenizer(),
						new Fields("words"))
				.parallelismHint(12)
				.each(new Fields("tweet_id", "text", "words", "hashtags"),
						new Vectorizer(), new Fields("tweet_obj"))
				.parallelismHint(12)
				.project(new Fields("tweet_obj"))
				// .each(new Fields("tweet_obj"), new Debug())
				.stateQuery(bucketsDB, new Fields("tweet_obj"),
						new BucketsStateUpdate(),
						new Fields("tweet_id", "collidingTweetsList"))
				.parallelismHint(ProjectConf.BucketsParallelism)
				.each(new Fields("tweet_obj", "collidingTweetsList"),
						new Debug());

		topology.newDRPCStream("tweets", drpc)
//				.parallelismHint(12)
				.each(new Fields("args"), new Preprocessor(),
						new Fields("text"))
				.each(new Fields("text"), new FakeID(), new Fields("tweet_id"))
				// .each(new Fields("text"), new Debug())
				.each(new Fields("text"), new Tokenizer(), new Fields("words"))
				.each(new Fields("tweet_id", "text", "words"),
						new Vectorizer(), new Fields("tweet_obj"))
				.project(new Fields("tweet_obj", "tweet_id"))
				// .each(new Fields("tweet_obj"), new Debug())
				.broadcast()
				.stateQuery(bucketsDB, new Fields("tweet_obj"),
						new BucketsStateQuery(),
						new Fields("collidingTweetsList"))
				.parallelismHint(ProjectConf.BucketsParallelism)
				.each(new Fields("collidingTweetsList"), new ExpandList(),
						new Fields("coltweet_obj", "coltweet_id"))
				.project(
						new Fields("tweet_id", "tweet_obj", "coltweet_obj",
								"coltweet_id"))
				.groupBy(new Fields("tweet_id", "coltweet_id"))
				.aggregate(
						new Fields("coltweet_id", "tweet_obj", "coltweet_obj"),
						new CountAggKeep(),
						new Fields("count", "tweet_obj", "coltweet_obj"))
				.groupBy(new Fields("tweet_id"))
				.aggregate(
						new Fields("count", "coltweet_id", "tweet_obj",
								"coltweet_obj"),
						new FirstNAggregator(3 * ProjectConf.L, "count", true),
						new Fields("countAfter", "coltweet_id", "tweet_obj",
								"coltweet_obj"))
				.each(new Fields("tweet_id", "coltweet_id", "tweet_obj",
						"coltweet_obj"), new ComputeDistance(),
						new Fields("cosSim"))
				.parallelismHint(ProjectConf.ComputeDistance)
				// .each(new Fields("tw_id", "coltweet_id", "cosSim"), new
				// Debug());
				.shuffle()
				.groupBy(new Fields("tweet_id"))
				// find ranked closest neighbor
				.aggregate(
						new Fields("coltweet_id", "tweet_obj", "coltweet_obj",
								"cosSim"),
						new FirstNAggregator(ProjectConf.L, "cosSim", true),
						new Fields("coltweet_id", "tweet_obj", "coltweet_obj",
								"cosSim"))
				.each(new Fields("cosSim"), new FilterLow())
				.each(new Fields("coltweet_obj"), new Extractor(),
						new Fields("tweet_text", "tweet_hashtags"))
				.project(new Fields("tweet_text", "tweet_hashtags", "cosSim"));
		return topology.build();
	}

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		List<String> servers = new ArrayList<String>();
		servers.add("ec2-54-215-207-12.us-west-1.compute.amazonaws.com");
		servers.add("ec2-54-215-220-17.us-west-1.compute.amazonaws.com");
		servers.add("ec2-54-215-220-27.us-west-1.compute.amazonaws.com");
		servers.add("ec2-54-215-203-118.us-west-1.compute.amazonaws.com");
		servers.add("ec2-54-183-80-147.us-west-1.compute.amazonaws.com");
		servers.add("ec2-54-215-175-197.us-west-1.compute.amazonaws.com");
//		servers.add("localhost");
		conf.put("drpc.servers", servers);
		conf.setNumWorkers(12);

		StormSubmitter.submitTopology("tweets", conf, buildTopology(null));

//		Config conf = new Config();
//		conf.setMaxSpoutPending(200);
//		if (args.length == 0) {
//			LocalDRPC drpc = new LocalDRPC();
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("hashtags", conf, buildTopology(drpc));
//			for (int i = 0; i < 1000; i++) {
//				Thread.sleep(10);
//				System.out.println("DRPC RESULT: " + i
//						+ drpc.execute("tweets", "Stick to the plan OPM"));
//			}
//		}
	}
}
