package hashtags.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import hashtags.ProjectConf;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * 
 * @author Xiaohu Chen
 * 
 *         A Redis bolt that consumes tweets from redis
 * 
 */

public class RedisSpout implements IBatchSpout {
	Jedis redis;
	BufferedReader br;
	int maxBatchSize;
	HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

	public RedisSpout() {
		this.maxBatchSize = ProjectConf.MAX_BATCH_SIZE;
		
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		this.redis = new Jedis(ProjectConf.REDIS_SERVER);
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
				while ((line = redis.rpop("tweets:tweets")) != null) {
					parser.parse(line);
					JSONObject obj = (JSONObject) JSONValue.parse(line);
					List<Object> tmp = new ArrayList<Object>();
					tmp.add(obj.get("tweet_id"));
					tmp.add(obj.get("text"));
					JSONArray hashtaglist = (JSONArray) obj.get("hashtags");
					// System.err.println("hashtaglist:" + hashtaglist);
					String hashtags = StringUtils.join(hashtaglist, ",");
					// System.err.println("hashtags:" + hashtags);
					tmp.add(hashtags);
					batch.add(tmp);
					index++;
					if (index >= maxBatchSize) {
						break;
					}
				}
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

	public static void main(String[] args) {
		RedisSpout client = new RedisSpout();
	}

}