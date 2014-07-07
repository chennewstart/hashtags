package hashtags.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import hashtags.ProjectConf;
import hashtags.utils.Tweet;
import hashtags.utils.SparseVector;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         This is responsible for converting the tweet object into a sparse
 *         vector based on previously seen terms.
 * 
 */
public class Vectorizer implements Function {
	private Jedis redis;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		redis = new Jedis(ProjectConf.REDIS_SERVER);
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// new Fields("tweet_id", "text", "words", "hashtags")

		String tweet_id = tuple.getString(0);
		String text = tuple.getString(1);
		Tweet tweet = new Tweet(tweet_id, text);
		List<String> words = (List<String>) tuple.getValue(2);
		if (tuple.size() == 4) {
			tweet.setHashtags((String) tuple.getValue(3));
		}
//		System.err.println("[Debug]tweet:" + tweet);
		Map<String, Long> tf = new HashMap<String, Long>();
		for (String word : words) {
			Long cnt = tf.get(word);
			if (cnt == null) {
				tf.put(word, (long) 1);
			} else {
				tf.put(word, cnt + 1);
			}
		}

		Integer total = Integer.valueOf(redis.get("d:total"));
		// System.err.println("[Debug]total:" + total);
		Pipeline p = redis.pipelined();
		for (String word : words) {
			p.hgetAll(word + ":token");
		}
		List<Object> results = p.syncAndReturnAll();
		// System.err.println(results.size());
		SparseVector vector = new SparseVector(total);

		int index = -1;
		for (Object result : results) {
//			System.err.println("result:" + result);
			index++;
			if (result == null) {
				continue;
			}
			Map<String, String> r = (Map<String, String>) result;
			if (r.get("tf") == null || r.get("position") == null) {
				continue;
			}
			Double df = Double.valueOf(r.get("tf"));
			Integer position = Integer.valueOf(r.get("position"));
			// System.err.println("df:" + df + ", position:" + position);
			double idf = 0.0;
			if (r.get("idf") == null) {
				idf = Math.log(total) / (df + 1);
			} else {
				idf = Double.valueOf(r.get("idf"));
			}
			// System.err.println("[Debug]word:" + words.get(index) + ", tf:"
			// + tf.get(words.get(index)) + ", idf:" + idf + ", position:" +
			// position);
			vector.set(position, tf.get(words.get(index)) * idf);
		}

		vector.trimToSize(); // very precious line, saves in performance
		vector = normalizeVector(vector);
		// vector.trimToSize();

		// idfs have been updated when constructing the vector
		tweet.setSparseVector(vector);
		// System.err.println("[Debug]vector:" + vector);
		collector.emit(new Values(tweet));
	}

	/**
	 * Normalization by dividing with Euclid norm.
	 * 
	 * @param vector
	 */
	public SparseVector normalizeVector(SparseVector vector) {
		// NORMALIZE HERE with norm1 so a unit vector is produced
		IntArrayList indexes = new IntArrayList(vector.cardinality());
		DoubleArrayList dbls = new DoubleArrayList(vector.cardinality());
		double norm = vector.getEuclidNorm();
		vector.getNonZeros(indexes, dbls);
		for (int i = 0; i < indexes.size(); i++) {
			vector.setQuick(indexes.get(i), dbls.getQuick(i) / norm);
		}
		return vector;
	}

	private Values getValues(Tweet tweet, String[] words, Long d, Long df[],
			Long pos[]) {
		// DEBUG
		if (words.length != pos.length) {
			System.err.println("words(" + words.length + "):");
			for (int i = 0; i < words.length; ++i) {
				System.err.println(i + " " + words[i]);
			}

			System.err.println("pos(" + pos.length + "):");
			for (int i = 0; i < pos.length; ++i) {
				System.err.println(i + " " + pos[i]);
			}
		}
		Map<String, Long> tf = new HashMap<String, Long>();
		for (String word : words) {
			Long cnt = tf.get(word);
			if (cnt == null) {
				tf.put(word, (long) 1);
			} else {
				tf.put(word, cnt + 1);
			}
		}

		int uniqWordsIncrease = 0;
		int size = 0;

		for (int i = 0; i < pos.length; ++i) {
			if (pos[i] < 0) {
				uniqWordsIncrease++;
				pos[i] = -pos[i];
			}
			if (pos[i] > size) {
				size = pos[i].intValue();
			}
		}

		Set<String> processed = new HashSet<String>();
		SparseVector vector = new SparseVector(size + 1);

		for (int i = 0; i < words.length; ++i) {
			String word = words[i];
			if (processed.contains(word)) {
				continue;
			}
			processed.add(word);
			vector.set(pos[i].intValue(), tf.get(word) * Math.log10(d)
					/ (df[i] + 1));
		}

		vector.trimToSize(); // very precious line, saves in performance

		vector = normalizeVector(vector);
		vector.trimToSize();

		// idfs have been updated when constructing the vector
		tweet.setSparseVector(vector);
		return (new Values(tweet, uniqWordsIncrease));
	}

	@Override
	public void cleanup() {

	}
}
