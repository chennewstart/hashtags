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
* Reducer class, update the new global minina for a vertex if necessary.
* input : key is vertexid, values is the vertexes that this vertex points to or a local minima
* output: key is vertexid, value has 3 fields: the vertexes that this vertex points to, current global minima 
*         and wether it is updated in this iteration
*/

public class ExplorationReducer extends
			Reducer<LongWritable, VertexWritable, LongWritable, Text> {

		public enum UpdateCounter {
  			UPDATED
 		}

		public void reduce(LongWritable key, Iterable<VertexWritable> values, Context context)
			throws IOException, InterruptedException {
			System.err.println("[key]" + key.get());
			System.err.println("[values]" + values);
			VertexWritable vertex = null;
			Long minimalVertexId = null;
			Iterator<VertexWritable> iterator = values.iterator();
			while (iterator.hasNext()) 
			{
				VertexWritable another = iterator.next();
				System.err.println("[another]" + another);
				if (another.isMessage())
				{
					if(minimalVertexId == null || minimalVertexId > another.minimalVertexId)
					{
						minimalVertexId = another.minimalVertexId;
					}
				}
				else
				{
					vertex = new VertexWritable();
					vertex.minimalVertexId = another.minimalVertexId;
					vertex.pointsTo = new TreeSet<Long>();
					Iterator<Long> adjs = another.pointsTo.iterator();
					while (adjs.hasNext())
					{
						vertex.pointsTo.add(adjs.next());	
					}
					System.err.println("[another is not Message]");
				}
			}
			System.err.println("[vertex]" + vertex);
			System.err.println("[minimalVertexId]" + minimalVertexId);
			boolean activated = false;
			assert(vertex != null);

			// if we found a new minimum activate the vertex and update the counter
			if(minimalVertexId != null && minimalVertexId < vertex.minimalVertexId)
			{
				vertex.minimalVertexId = minimalVertexId;
				activated = true;
			}
			if(activated)
			{
				context.getCounter(UpdateCounter.UPDATED).increment(1);
			}
			
			// 3 fields: the vertexes that this vertex points to, current global minima and wether it is updated in this iteration
			StringBuilder sb = new StringBuilder();
			Iterator<Long> adjs = vertex.pointsTo.iterator();
			while (adjs.hasNext())
			{
				sb.append(adjs.next());
				if(adjs.hasNext())
				{
					sb.append(" ");
				}
			}
			sb.append(",");
			sb.append(vertex.minimalVertexId);
			sb.append(",");
			sb.append(activated? 1 : 0);
			Text value = new Text();
			value.set(sb.toString());
			context.write(key, value);
		}
	}