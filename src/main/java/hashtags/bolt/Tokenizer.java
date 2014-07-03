package hashtags.bolt;

import hashtags.ProjectConf;
import hashtags.utils.TweetBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class Tokenizer implements Function {
	private final String regex = "[^A-Za-z0-9_£$%<>]";
	private TweetBuilder tb;

	@Override
	public void prepare(Map arg0, TridentOperationContext arg1) {
		tb = new TweetBuilder(ProjectConf.PATH_TO_OOV_FILE);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String tweetBody = tuple.getString(0);
		String words[] = tweetBody.toLowerCase().split(regex);
		String temp;
		List<String> wordlist = new ArrayList<String>();
		for (String word : words) {
			if (word.equals("")) {
				continue;
			}
			temp = tb.getOOVNormalWord(word);
			if (temp != null) {
				word = temp;
			}
			wordlist.add(word);
		}

		collector.emit(new Values(wordlist));
	}

	@Override
	public void cleanup() {

	}
}
