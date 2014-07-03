package hashtags.state;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

/**
 * 
 * @author Xiaohu Chen
 * 
 * posState Factory
 *
 */

public class PosStateFactory implements StateFactory {

	public PosStateFactory() {
	}

	@Override
	public State makeState(Map conf, IMetricsContext metrics,
			int partitionIndex, int numPartitions) {
		return new PosDB();
	}

}