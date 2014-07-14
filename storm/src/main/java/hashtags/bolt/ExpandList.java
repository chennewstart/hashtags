package hashtags.bolt;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import hashtags.utils.Tweet;

/**
 * Expand a list of tweets and emit tweet one by one 
 *
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 *
 */
public class ExpandList extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// System.err.println("[Debug]ExpandList" + tuple.getValue(0));
		@SuppressWarnings("rawtypes")
		List<Tweet> tweets = (List<Tweet>) tuple.getValue(0);
		if (tweets != null) {
			for (Tweet tweet : tweets) {
				collector.emit(new Values(tweet, tweet.getID()));
			}
		}
	}
}
