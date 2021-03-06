package hashtags.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.state.State;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         Query dfState to get the term frequency
 * 
 */
public class DFStateUpdateQuery implements QueryFunction<State, List<Long>> {

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map conf, TridentOperationContext arg1) {

	}

	@Override
	public List<List<Long>> batchRetrieve(State dfState,
			List<TridentTuple> tuples) {
		Map<String, Long> freqs = new HashMap<String, Long>();
		for (TridentTuple tuple : tuples) {
			List<String> words = (List<String>) tuple.get(0);
			Set<String> uniqueWords = new HashSet<String>(words);
			for (String word : uniqueWords) {
				Long freq = freqs.get(word);
				if (freq == null) {
					freq = (long) 1;
				} else {
					freq = freq + 1;
				}
				freqs.put(word, freq);
			}
		}

		List<List<Object>> keys = new ArrayList<List<Object>>();
		for (Map.Entry<String, Long> entry : freqs.entrySet()) {
			List<Object> key = new ArrayList<Object>();
			String word = entry.getKey();
			key.add(word);
			keys.add(key);
		}

		List<Long> vals = ((MemoryMapState<Long>) dfState).multiGet(keys);

		for (int i = 0; i < keys.size(); ++i) {
			String word = (String) keys.get(i).get(0);
			Long val = vals.get(i);
			if (val == null) {
				val = freqs.get(word);
			} else {
				val = val + freqs.get(word);
			}

			freqs.put(word, val);
			vals.set(i, val);
		}

		((MemoryMapState<Long>) dfState).multiPut(keys, vals);

		List<List<Long>> dfs = new ArrayList<List<Long>>();

		for (TridentTuple tuple : tuples) {
			List<Long> df = new ArrayList<Long>();
			List<String> words = (List<String>) tuple.get(0);
			for (String word : words) {
				df.add(freqs.get(word));
			}
			dfs.add(df);
		}

		return dfs;
	}

	@Override
	public void execute(TridentTuple tuple, List<Long> df,
			TridentCollector collector) {
		collector.emit(new Values(df));
	}

}
