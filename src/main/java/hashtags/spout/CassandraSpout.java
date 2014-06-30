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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class CassandraSpout implements IBatchSpout {
	private Cluster cluster;
	BufferedReader br;
	int maxBatchSize;
	HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

	public CassandraSpout(int maxBatchSize) {
		this.maxBatchSize = maxBatchSize;
	}

	boolean cycle = false;

	public void setCycle(boolean cycle) {
		this.cycle = cycle;
	}

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
	}

	@Override
	public void open(Map conf, TopologyContext context) {
		br = new BufferedReader(new InputStreamReader(
				CassandraSpout.class.getResourceAsStream("/sample.txt")));
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
		cluster.close();
	}

	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("tweet_id", "oldtext", "hashtags");
	}

	public static void main(String[] args) {
		CassandraSpout client = new CassandraSpout(100);
		client.connect("ec2-54-187-166-118.us-west-2.compute.amazonaws.com");
		client.close();
	}

}