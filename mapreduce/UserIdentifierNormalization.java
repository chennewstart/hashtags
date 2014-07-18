import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Arrays;
import java.util.TreeSet;
import org.apache.hadoop.fs.FileSystem;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

// JDK API docs : http://docs.oracle.com/javase/7/docs/api/
// Hadoop API docs : http://hadoop.apache.org/docs/stable/api/

public class UserIdentifierNormalization extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UserIdentifierNormalization(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// if (args.length != 2) {
		// 	System.out.println("usage : need <input path>  <output path>");
		// 	return 1;
		// }

		// variable to keep track of the recursion depth
		int depth = 0;
		// counter from the previous running import job
		long counter = 1;
  		
  		depth++;
  		
  		Configuration conf = getConf();
  		conf.set("mapred.textoutputformat.separator", ",");
  		// conf.set("mapreduce.textoutputformat.separator", ",");  //Hadoop v2+ (YARN)

  		FileSystem fs = FileSystem.get(conf);
  		while (counter > 0) 
  		{
  			Path inputPath = new Path("xiaohu/graph/depth_" + (depth - 1) + "/");
			Path outputPath = new Path("xiaohu/graph/depth_" + depth);

			conf.set("recursion.depth", depth + "");
			Job job = Job.getInstance(conf);
			// Specify various job-specific parameters     
     		job.setJobName(getClass().getName() + "--xiaohu" + depth);
			job.setJarByClass(UserIdentifierNormalization.class);
		
			job.setMapperClass(ExplorationMapper.class);
			job.setReducerClass(ExplorationReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(VertexWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			TextInputFormat.setInputPaths(job, inputPath);

			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, outputPath);

			// delete the outputPath if already exists
			if (fs.exists(outputPath))
			{
				fs.delete(outputPath, true);
			}

			job.waitForCompletion(true);
			depth++;
			counter = job.getCounters().findCounter(ExplorationReducer.UpdateCounter.UPDATED).getValue();
  		}

		return 0;
	}
}
