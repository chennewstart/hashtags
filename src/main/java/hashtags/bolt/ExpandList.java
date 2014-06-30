package hashtags.bolt;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import hashtags.utils.Tweet;

public class ExpandList extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		@SuppressWarnings("rawtypes")
		List<Tweet> tweets = (List<Tweet>) tuple.getValue(0);
		if (tweets != null) {
			for (Tweet tweet : tweets) {
				collector.emit(new Values(tweet, tweet.getID()));
			}
		}
	}

}
