package hashtags;

/***
 * 
 * @author Xiaohu Chen
 * 
 * Project Configuration 
 *
 */

public class ProjectConf {
	public static final String REDIS_HOST_KEY = "redisHost";
	public static final String REDIS_PORT_KEY = "redisPort";
	public static final String DEFAULT_JEDIS_PORT = "6379";
	public static final int maxBatchSize = 100;
	public static final String PATH_TO_OOV_FILE = "/oov.txt";
	public static final int UNIQUE_WORDS_EXPECTED = 500000;
	// maximum number of neighbours per bucket
	public static final int QUEUE_SIZE = 20;
	// buckets
	public static final int L = 36;
	// hash bits
	public static final int k = 13;
	// Parallelism of components - BucketParallelism should be lower than the
	// number of buckets, as each thread
	// should hold at least one bucket
	public static final int BucketsParallelism = 18;

	public static final int ComputeDistance = 12;

}