package hashtags.bolt;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class FilterLow extends BaseFilter {
	public boolean isKeep(TridentTuple tuple) {
		return tuple.getDouble(0) > 0.0;
	}
}