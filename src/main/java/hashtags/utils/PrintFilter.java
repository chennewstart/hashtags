package hashtags.utils;

import java.util.Map;

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class PrintFilter implements Filter {

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
	}

	@Override
	public void cleanup() {
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		System.out.println(tuple);
		return true;
	}
}
