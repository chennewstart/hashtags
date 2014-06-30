package hashtags.state;

import hashtags.ProjectConf;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class BucketsStateFactory implements StateFactory {

	int partialL, k, queueSize;

	public BucketsStateFactory() {
		// partial L will give the number of buckets each thread (given by
		// parallelism hint) will hold
		this.partialL = ProjectConf.L / ProjectConf.BucketsParallelism;
		this.k = ProjectConf.k;
		queueSize = ProjectConf.QUEUE_SIZE;
	}

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		return new BucketsDB(partialL, k, queueSize);
	}

}