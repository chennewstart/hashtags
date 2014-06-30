package hashtags;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import hashtags.bolt.ComputeDistance;
import hashtags.bolt.CountAggKeep;
import hashtags.bolt.ExpandList;
import hashtags.bolt.Extractor;
import hashtags.bolt.FakeID;
import hashtags.bolt.FilterLow;
import hashtags.bolt.FirstNAggregator;
import hashtags.bolt.Preprocessor;
import hashtags.bolt.Tokenizer;
import hashtags.bolt.VectorBuilder;
import hashtags.bolt.Vectorizer;
import hashtags.bolt.WrapToTweet;
import hashtags.spout.TweetSpout;
import hashtags.state.BucketsStateFactory;
import hashtags.state.BucketsStateQuery;
import hashtags.state.BucketsStateUpdateQuery;
import hashtags.state.DFStateQuery;
import hashtags.state.DFStateUpdateQuery;
import hashtags.state.DStateQuery;
import hashtags.state.DStateUpdateQuery;
import hashtags.state.PosStateFactory;
import hashtags.state.PosStateQuery;
import hashtags.state.PosStateUpdateQuery;

public class HashtagTopology {

	public static StormTopology buildTopology(LocalDRPC drpc) {

		TweetSpout spout = new TweetSpout(ProjectConf.maxBatchSize);
		TridentTopology topology = new TridentTopology();

		TridentState bucketsDB = topology
				.newStaticState(new BucketsStateFactory());
		TridentState dState = topology
				.newStaticState(new MemoryMapState.Factory());
		TridentState dfState = topology
				.newStaticState(new MemoryMapState.Factory());
		TridentState posState = topology.newStaticState(new PosStateFactory());
		Stream docstream = topology
				.newStream("spout1", spout)
				.parallelismHint(4)
				// .each(new Fields("tweet_id", "text", "hashtags"), new
				// Debug());
				.each(new Fields("text"), new Preprocessor(),
						new Fields("cleantext"))
				// .project(new Fields("tweet_id", "text", "hashtags"))
				.each(new Fields("cleantext"), new Tokenizer(),
						new Fields("words"))
				.stateQuery(dState, new Fields("text"),
						new DStateUpdateQuery(), new Fields("d"))
				// .each(new Fields("tweet_id", "text", "hashtags", "words",
				// "d"), new Debug())
				.stateQuery(dfState, new Fields("words"),
						new DFStateUpdateQuery(), new Fields("df"))
				.stateQuery(posState, new Fields("words"),
						new PosStateUpdateQuery(), new Fields("pos"))
				// .each(new Fields("tweet_id", "text", "hashtags", "words",
				// "d",
				// "df", "pos"), new Debug())
				.each(new Fields("tweet_id", "text", "hashtags", "words", "d",
						"df", "pos"), new Vectorizer(),
						new Fields("tweet_obj", "uniqWordsIncrease"))
				.project(new Fields("tweet_obj", "uniqWordsIncrease"))
				// .each(new Fields("tweet_obj", "uniqWordsIncrease"), new
				// Debug());
				.broadcast()
				.stateQuery(bucketsDB,
						new Fields("tweet_obj", "uniqWordsIncrease"),
						new BucketsStateUpdateQuery(),
						new Fields("tweet_id", "collidingTweetsList"));
		// .each(new Fields("tweet_obj", "collidingTweetsList"),
		// new Debug());

		topology.newDRPCStream("tweets", drpc)
				.parallelismHint(4)
				.each(new Fields("args"), new Preprocessor(),
						new Fields("text"))
				.each(new Fields("text"), new FakeID(), new Fields("tweet_id"))
				// .each(new Fields("text"), new Debug());
				.each(new Fields("text"), new Tokenizer(), new Fields("words"))
				// .each(new Fields("text", "words"), new Debug());
				.stateQuery(dState, new Fields("text"), new DStateQuery(),
						new Fields("d"))
				// .each(new Fields("d"), new Debug());
				.stateQuery(dfState, new Fields("words"), new DFStateQuery(),
						new Fields("df"))
				// .each(new Fields("words", "df"), new Debug());
				.stateQuery(posState, new Fields("words"), new PosStateQuery(),
						new Fields("pos"))
				// .each(new Fields("tweet_id", "text", "hashtags", "words",
				// "d", "df", "pos"), new Debug());
				.each(new Fields("text", "words", "d", "df", "pos"),
						new Vectorizer(), new Fields("vector"))
				// .project(new Fields("tweet_obj", "uniqWordsIncrease"))
				// // .each(new Fields("tweet_obj", "uniqWordsIncrease"), new
				// // Debug());
				.broadcast()
				.stateQuery(bucketsDB, new Fields("vector"),
						new BucketsStateQuery(),
						new Fields("collidingTweetsList"))
				.parallelismHint(ProjectConf.BucketsParallelism)
				.each(new Fields("collidingTweetsList"), new ExpandList(),
						new Fields("coltweet_obj", "coltweet_id"))
				.each(new Fields("tweet_id", "text", "vector"),
						new WrapToTweet(), new Fields("tweet_obj"))
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
				// find closest neighbor
				.aggregate(
						new Fields("coltweet_id", "tweet_obj", "coltweet_obj",
								"cosSim"),
						new FirstNAggregator(ProjectConf.L, "cosSim", true), // give
																				// only
						// the
						// closest
						// neighbor
						new Fields("coltweet_id", "tweet_obj", "coltweet_obj",
								"cosSim"))
				.each(new Fields("cosSim"), new FilterLow())
				.each(new Fields("coltweet_obj"), new Extractor(), new Fields("tweet_text", "tweet_hashtags"))
				.project(new Fields("tweet_text", "tweet_hashtags", "cosSim"));
		// .each(new Fields("coltweet_obj"), new Extractor(),
		// new Fields("tweet_text", "tweet_hashtags"))
		// .project(new Fields("tweet_text", "tweet_hashtags"));

		// .each(new Fields("sentence"), new Split(), new Fields("word"))
		// .groupBy(new Fields("word"))
		// .persistentAggregate(new MemoryMapState.Factory(), new Count(),
		// new Fields("count")).parallelismHint(16);
		//
		// topology.newDRPCStream("words", drpc)
		// .each(new Fields("args"), new Split(), new Fields("word"))
		// .groupBy(new Fields("word"))
		// .stateQuery(wordCounts, new Fields("word"), new MapGet(),
		// new Fields("count"))
		// .each(new Fields("count"), new FilterNull())
		// .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		return topology.build();
	}

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
    	List<String> servers = new ArrayList<String>();
    	servers.add("localhost");
    	conf.put("drpc.servers", servers);

    	StormSubmitter.submitTopology("tweets", conf, buildTopology(null));

//		Config conf = new Config();
//		conf.setMaxSpoutPending(20);
//		if (args.length == 0) {
//		LocalDRPC drpc = new LocalDRPC();
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("hashtags", conf, buildTopology(drpc));
//		for (int i = 0; i < 1000; i++) {
//		 		Thread.sleep(100000);
//		 		System.out.println("DRPC RESULT: "
//		 				+ drpc.execute("tweets", "Stick to the plan OPM"));
//		 	}
//		 }
	}
}
