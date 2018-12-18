package hadoop.mr.tests.customformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

//Identity strategy because the goal is to real the XML and get a structured file only
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
