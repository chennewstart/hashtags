package hashtags.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.state.State;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class DStateQuery implements QueryFunction<State, Long> {

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map arg0, TridentOperationContext arg1) {

	}

	@Override
	public List<Long> batchRetrieve(State dState, List<TridentTuple> tuples) {
		List<Long> ds = new ArrayList<Long>();
		List<List<Object>> keys = new ArrayList<List<Object>>();
		for (TridentTuple tuple : tuples) {
			List<Object> key = new ArrayList<Object>();
			key.add("d");
			keys.add(key);
		}
		List<Long> vals = ((MemoryMapState<Long>) dState).multiGet(keys);
		Long val = vals.get(0);
		for (TridentTuple tuple : tuples) {
			if (val == null) {
				ds.add((long) 0);
			} else {
				ds.add(val);
			}
		}
		return ds;
	}

	@Override
	public void execute(TridentTuple tuple, Long d, TridentCollector collector) {
		collector.emit(new Values(d));
	}

}
