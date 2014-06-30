package hashtags.bolt;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import hashtags.utils.SparseVector;
import hashtags.utils.Tweet;

public class WrapToTweet extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// new Fields("tweet_id", "text", "vector"),
		Long tweet_id = tuple.getLong(0);
		String text = tuple.getString(1);
		SparseVector vector = (SparseVector) tuple.getValue(2);
		Tweet tweet = new Tweet(tweet_id, text);
		tweet.setSparseVector(vector);
		collector.emit(new Values(tweet));
	}

}
