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
public class BucketsStateUpdateQuery extends
		BaseQueryFunction<BucketsDB, ArrayList<Tweet>> {

	@Override
	public List<ArrayList<Tweet>> batchRetrieve(BucketsDB state,
			List<TridentTuple> tuples) {
		List<ArrayList<Tweet>> tweets = new ArrayList<ArrayList<Tweet>>();
		for (TridentTuple tuple : tuples) {
			Tweet tw = (Tweet) tuple.getValue(0);
			int byNumberOfDims = tuple.getInteger(1); // number of dimensions to
														// upgrade
			state.updateRandomVectors(byNumberOfDims);
			ArrayList<Tweet> possibleNeighbors = state.getPossibleNeighbors(tw);
			tweets.add(possibleNeighbors);
		}

		return tweets;
	}

	@Override
	public void execute(TridentTuple tuple, ArrayList<Tweet> collidingTweets,
			TridentCollector collector) {
		// emit by tweet id
		Tweet tw = (Tweet) tuple.getValue(0);
		collector.emit(new Values(tw.getID(), collidingTweets));
	}

}
