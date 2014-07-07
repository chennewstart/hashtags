package hashtags.bolt;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         A simple filter function that filters tweets whose cosine similarity
 *         is too low
 * 
 */
public class FilterLow extends BaseFilter {
	public boolean isKeep(TridentTuple tuple) {
		return tuple.getDouble(0) > 0;
	}
}