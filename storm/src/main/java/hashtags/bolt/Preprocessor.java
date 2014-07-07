package hashtags.bolt;

import hashtags.ProjectConf;
import hashtags.utils.Tools;
import hashtags.utils.TweetBuilder;

import java.util.Map;

import org.apache.log4j.Logger;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * Processes the tweet text to remove whitespaces, links and replies.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * 
 */
public class Preprocessor extends BaseFunction {

	private TweetBuilder tb;
	private Tools tools;
	private static final Logger LOG = Logger.getLogger(Preprocessor.class);

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		tools = new Tools();
		tb = new TweetBuilder(ProjectConf.PATH_TO_OOV_FILE);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String tweetText = null;
		try {
			tweetText = tools.clean(tb.removeSpacesInBetween((String) tuple
					.getValue(0)));
		} catch (Exception e) {
			LOG.error(e.toString());
		}
		collector.emit(new Values(tweetText));

	}

}
