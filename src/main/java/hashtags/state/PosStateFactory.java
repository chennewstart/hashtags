package hashtags.state;

import hashtags.ProjectConf;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

public class PosStateFactory implements StateFactory {

	int partialL, k, queueSize;

	public PosStateFactory() {
	}

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		return new PosDB();
	}

}