package hashtags.bolt;

import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import hashtags.utils.Tools;

import hashtags.utils.NearNeighbour;
import hashtags.utils.Tweet;

/**
 * Computes the cosine similarity between two tweets and emits the value.
 * 
 */
public class ComputeDistance implements Function {

	Tools tools;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		tools = new Tools();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Tweet possibleNeighbour = (Tweet) tuple.getValueByField("coltweet_obj");
		Tweet newTweet = (Tweet) tuple.getValueByField("tweet_obj");
		NearNeighbour possibleClosest = tools.computeCosineSimilarity(
				possibleNeighbour, newTweet);

		collector.emit(new Values(possibleClosest.getCosine()));
	}

	@Override
	public void cleanup() {

	}

}
