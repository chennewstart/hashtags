package hashtags.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import hashtags.ProjectConf;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class Test {
	public static void main(String[] args) {
		Jedis redis = new Jedis(ProjectConf.REDIS_SERVER);
		Double total = Double.valueOf(redis.get("d:total"));
		List<String> words = new ArrayList<String>();
		words.add("what");
		words.add("am");
		words.add("i'm");
		words.add("doing");
		Pipeline p = redis.pipelined();
		for (String word : words)
		{
			p.hgetAll(word + ":token");
		}
		List<Object> results = p.syncAndReturnAll();
		System.out.println(results.size());
		for(Object result : results)
		{
			System.out.println("result:" + result);
			if (result == null)
			{
				continue;
			}
			Map<String, String> r = (Map<String, String>)result;
			if (r.get("tf") == null)
			{
				continue;
			}
			Double df = Double.valueOf(r.get("tf"));
			double idf = 0.0;
			
			if (r.get("idf") == null)
			{
				idf = Math.log(total) / (df + 1);
			}
			else
			{
				idf = Double.valueOf(r.get("idf"));
				
			}
			System.out.println("idf:" + idf);
		}
	}
}