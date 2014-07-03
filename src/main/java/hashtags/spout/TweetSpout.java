package hashtags.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author xiaohu Chen
 * 
 *         A simulated tweet spout that read tweets from a large text file in
 *         JSON format and feeds to the trident topology
 * 
 */
public class TweetSpout implements IBatchSpout {
	BufferedReader br;
	int maxBatchSize;
	HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

	public TweetSpout(int maxBatchSize) {
		this.maxBatchSize = maxBatchSize;
	}

	boolean cycle = false;

	public void setCycle(boolean cycle) {
		this.cycle = cycle;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		br = new BufferedReader(new InputStreamReader(
				TweetSpout.class.getResourceAsStream("/sample.txt")));
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batch = this.batches.get(batchId);
		if (batch == null) {
			batch = new ArrayList<List<Object>>();
			String line;
			int index = 0;
			JSONParser parser = new JSONParser();
			try {
				while ((line = br.readLine()) != null) {
					parser.parse(line);
					JSONObject obj = (JSONObject) JSONValue.parse(line);
					List<Object> tmp = new ArrayList<Object>();
					tmp.add(obj.get("tweet_id"));
					tmp.add(obj.get("text"));
					tmp.add(obj.get("hashtags"));
					batch.add(tmp);
					index++;
					if (index >= maxBatchSize) {
						break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			this.batches.put(batchId, batch);
		}
		for (List<Object> list : batch) {
			collector.emit(list);
		}
	}

	@Override
	public void ack(long batchId) {
		this.batches.remove(batchId);
	}

	@Override
	public void close() {
	}

	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("tweet_id", "text", "hashtags");
	}

}