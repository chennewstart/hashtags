package hashtags.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import storm.trident.state.State;

/**
 * Holds a list of buckets and a list of random vectors.
 * 
 */
public class PosDB implements State, Serializable {
	Map<String, Long> posMap;

	public PosDB() {
		posMap = new HashMap<String, Long>();
	}

	@Override
	public void beginCommit(Long arg0) {

	}

	@Override
	public void commit(Long arg0) {

	}

	public Long getPos(String word) {
		Long pos = posMap.get(word);
		return pos;
	}

	public Long addPos(String word) {
		Long pos = (long) posMap.size();
		posMap.put(word, pos);
		return pos;
	}
}