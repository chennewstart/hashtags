package hashtags.state;

import java.util.ArrayList;
import java.util.List;

import hashtags.utils.SparseVector;
import hashtags.utils.Tweet;
import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         Query bucket state to find similar tweets but doesn't update the
 *         bucket state
 * 
 */
public class BucketsStateQuery extends
		BaseQueryFunction<BucketsDB, ArrayList<Tweet>> {

	@Override
	public List<ArrayList<Tweet>> batchRetrieve(BucketsDB state,
			List<TridentTuple> tuples) {
		List<ArrayList<Tweet>> tweets = new ArrayList<ArrayList<Tweet>>();
		for (TridentTuple tuple : tuples) {
			Tweet tweet = (Tweet) tuple.getValue(0);
			ArrayList<Tweet> possibleNeighbors = state
					.getPossibleNeighbors(tweet, false);
			tweets.add(possibleNeighbors);
		}

		return tweets;
	}

	@Override
	public void execute(TridentTuple tuple, ArrayList<Tweet> collidingTweets,
			TridentCollector collector) {
		Tweet tw = (Tweet) tuple.getValue(0);
		collector.emit(new Values(tw.getID(), collidingTweets));
	}

}
