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

/**
* Mapper class, pass the connectivity information and broadcast new minina if the vertex is activated (found new global minina).
* input : key is line number (or offset, unused); 
*         Depends on whether it is the first iteration, value can either be a vertex and all the vertexes that it points to (first iteration)
*         or the vertex, all the vertexes that it points to as well as wehter it's global minima is updated
* output: key is the vertexId, value is the vertexes that it points to or local minima
*/

public class ExplorationMapper extends Mapper<Object, Text, LongWritable, VertexWritable> {
		
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context) throws IOException {
			
			try {
				String[] tokens = value.toString().split(",");
				System.err.println("[key]" + key);
				System.err.println("[value]" + value.toString());
				System.err.println("[tokens size]" + tokens.length);
				if (tokens.length == 1)
				{
					StringTokenizer vertexes = new StringTokenizer(tokens[0]);
					Long vertexId = Long.parseLong(vertexes.nextToken());
					TreeSet<Long> pointsTo = new TreeSet<Long>();
					while (vertexes.hasMoreTokens())
					{
						pointsTo.add(Long.parseLong(vertexes.nextToken()));
					}
					System.err.println("[vertexId]" + vertexId);
					System.err.println("[pointsTo size]" + pointsTo.size());
					Long minimalVertexId = vertexId;
					if(pointsTo.size() > 0 && pointsTo.first() < minimalVertexId)
					{
						minimalVertexId = pointsTo.first();
					}
					VertexWritable vertex = new VertexWritable();
					vertex.minimalVertexId = vertexId;
					vertex.pointsTo = pointsTo;
					System.err.println("[context.write]key " + vertexId.toString() + " value " + vertex);
					context.write(new LongWritable(vertexId), vertex);
					VertexWritable message = new VertexWritable();
					message.minimalVertexId = minimalVertexId;
					message.pointsTo = null;
					context.write(new LongWritable(vertexId), message);
					Iterator<Long> iterator = pointsTo.iterator();
					while (iterator.hasNext())
					{
						// message = new VertexWritable();
						// message.minimalVertexId = minimalVertexId;
						// message.pointsTo = null;
						Long tmp = iterator.next();
						System.err.println("[context.write]key " + tmp.toString() + " value " + message);
						context.write(new LongWritable(tmp), message);
					}
				}
				else
				{
					// assert(tokens.countTokens() == 4);
					Long vertexId = Long.parseLong(tokens[0]);
					TreeSet<Long> pointsTo = new TreeSet<Long>();
					StringTokenizer vertexes = new StringTokenizer(tokens[1]);
					while(vertexes.hasMoreTokens())
					{
						pointsTo.add(Long.parseLong(vertexes.nextToken()));
					}
					Long minimalVertexId = Long.parseLong(tokens[2]);
					boolean activated = tokens[3].equals("1");

					System.err.println("[vertexId]" + vertexId);
					System.err.println("[pointsTo size]" + pointsTo.size());
					System.err.println("[minimalVertexId]" + minimalVertexId);
					System.err.println("[activated]" + activated);

					VertexWritable vertex = new VertexWritable();
					vertex.minimalVertexId = minimalVertexId;
					vertex.pointsTo = pointsTo;
					System.err.println("[context.write]key " + vertexId.toString() + " value " + vertex);
					context.write(new LongWritable(vertexId), vertex);
					
					// If a vertex is activated, loop through the pointsTo tree and write a message with 
					// the minimal vertex to every element of the tree
					if(activated)
					{
						System.err.println("[activated!!]");
						Iterator<Long> iterator = pointsTo.iterator();
						while (iterator.hasNext())
						{
							VertexWritable message = new VertexWritable();
							message.minimalVertexId = minimalVertexId;
							message.pointsTo = null;
							Long tmp = iterator.next();
							System.err.println("[context.write]key " + tmp.toString() + " value " + message);
							context.write(new LongWritable(tmp), message);
						}
					}
				}
			} catch (Exception e) {
				System.out.println("*** exception:");
				e.printStackTrace();
			}
		}

	}