package hashtags.bolt;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class FakeID extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String text = tuple.getString(0);
		Long hash = (long) 7;
		for (int i = 0; i < text.length(); i++) {
			hash = hash * 31 + text.charAt(i);
		}
		collector.emit(new Values(hash));
	}
}
