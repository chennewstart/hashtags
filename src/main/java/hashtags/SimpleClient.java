package hashtags;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

/**
 * Hello world!
 * 
 */
public class SimpleClient {
	public static void main(String[] args) throws TException,
			DRPCExecutionException {
		DRPCClient client = new DRPCClient("localhost", 3772);
		System.out.println("client finished");
		String result = client.execute("tweets", "Stick to the plan OPM");
		System.out.println(result);
	}
}
