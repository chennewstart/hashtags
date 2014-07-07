package hashtags;

/***
 * 
 * @author Xiaohu Chen
 * 
 * Project Configuration 
 *
 */

public class ProjectConf {
	public static final String REDIS_SERVER = "ec2-54-187-166-118.us-west-2.compute.amazonaws.com";
	public static final String DEFAULT_JEDIS_PORT = "6379";
	public static final int MAX_BATCH_SIZE = 100;
	public static final int UNIQUE_WORDS_EXPECTED = 500000;
	public static final String PATH_TO_OOV_FILE = "/oov.txt";
	// maximum number of neighbours per bucket
	public static final int QUEUE_SIZE = 200;
	// buckets
	public static final int L = 36;
	// hash bits
	public static final int K = 13;
	// Parallelism of components - BucketParallelism should be lower than the
	// number of buckets, as each thread
	// should hold at least one bucket
	public static final int BucketsParallelism = 18;

	public static final int ComputeDistance = 12;

}