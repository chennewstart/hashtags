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
 * 
 *
 */
public class PosStateQuery implements QueryFunction<State, List<Long>> {

	@Override
	public void cleanup() {

	}

	@Override
	public void prepare(Map conf, TridentOperationContext arg1) {

	}

	@Override
	public List<List<Long>> batchRetrieve(State posState,
			List<TridentTuple> tuples) {

		Set<String> uniqueWords = new HashSet<String>();
		for (TridentTuple tuple : tuples) {
			List<String> words = (List<String>) tuple.get(0);

			for (String word : words) {
				uniqueWords.add(word);
			}
		}

		Map<String, Long> posMap = new HashMap<String, Long>();
		for (String word : uniqueWords) {
			Long pos = ((PosDB) posState).getPos(word);
			if (pos == null) {
				pos = (long) -1;
			}
			posMap.put(word, pos);
		}

		List<List<Long>> poss = new ArrayList<List<Long>>();

		for (TridentTuple tuple : tuples) {
			List<Long> pos = new ArrayList<Long>();
			List<String> words = (List<String>) tuple.get(0);
			for (String word : words) {
				Long val = posMap.get(word);
				pos.add(val);
			}
			poss.add(pos);
		}

		return poss;
	}

	@Override
	public void execute(TridentTuple tuple, List<Long> pos,
			TridentCollector collector) {
		collector.emit(new Values(pos));
	}

}
