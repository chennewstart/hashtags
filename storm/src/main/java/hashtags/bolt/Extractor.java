package hashtags.bolt;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import hashtags.utils.Tweet;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         Given a tweet, extract its text and hashtags
 * 
 */
public class Extractor extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Tweet tweet = (Tweet) tuple.getValue(0);
		collector.emit(new Values(tweet.getText(), tweet.getHashtags()));
	}

}
