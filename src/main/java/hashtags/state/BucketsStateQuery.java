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
 * Holds the state for a number of buckets. Each bucket will return near
 * neighbours that their hash collide with the tweet in question.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * 
 */
public class BucketsStateQuery extends
		BaseQueryFunction<BucketsDB, ArrayList<Tweet>> {

	@Override
	public List<ArrayList<Tweet>> batchRetrieve(BucketsDB state,
			List<TridentTuple> args) {
		List<ArrayList<Tweet>> tweets = new ArrayList<ArrayList<Tweet>>();
		for (TridentTuple tuple : args) {
			SparseVector vector = (SparseVector) tuple.getValue(0);
			ArrayList<Tweet> possibleNeighbors = state
					.getPossibleNeighbors(vector);
			tweets.add(possibleNeighbors);
		}

		return tweets;
	}

	@Override
	public void execute(TridentTuple tuple, ArrayList<Tweet> collidingTweets,
			TridentCollector collector) {
		// emit by tweet id
		collector.emit(new Values(collidingTweets));
	}

}
