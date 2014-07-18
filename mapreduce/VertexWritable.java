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
* Value class that represents a vertex (pass the graph structer information) and a message (pass the local minima message) at the same time
* if it represents a vertex, then it has 'pointsTo' field which is the vertexes that this vertex points to
* othewise, 'pointsTo' is null.
*
*/

public class VertexWritable implements Writable, Cloneable {
	Long minimalVertexId = null;
	TreeSet<Long> pointsTo = null;
	// boolean activated;

	// return whether it represents a message or not
	public boolean isMessage() 
	{
		if (pointsTo == null)
			return true;
		else
			return false;
	}

	// implements Writable interface
	public void write(DataOutput out) throws IOException {
		out.writeLong(minimalVertexId);
		if (pointsTo != null)
		{
			out.writeInt(pointsTo.size());
			Iterator<Long> iterator = pointsTo.iterator();
			while (iterator.hasNext())
			{
				out.writeLong(iterator.next());
			}
		}
		else
		{
			out.writeInt(-1);
		}
	}

	// implements Writable interface
	public void readFields(DataInput in) throws IOException {
		minimalVertexId = in.readLong();
		int size = in.readInt();
		if (size >= 0)
		{
			pointsTo = new TreeSet<Long>();
			for(int i = 0; i < size; ++i)
			{
				pointsTo.add(in.readLong());
			}
		}
		else
		{
			pointsTo = null;
		}
	}

	// for debug purpose
	@Override public String toString() {
		StringBuilder result = new StringBuilder();
		if (pointsTo == null)
		{
			result.append("null");
		}
		else
		{
			for (Long p : pointsTo)
			{
				result.append(p);
				result.append(",");
			}
		}
		result.append(":");
		result.append(minimalVertexId);
		return result.toString();
	}

	   // public static VertexWritable read(DataInput in) throws IOException {
	   //   VertexWritable w;
	   //   w.readFields(in);
	   //   return w;
	   // }
	}