package hadoop.mr.tests.customformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path inputDataset = new Path("file:////home/rodrir23/data-test/xml.txt/");
		Path outputDir = new Path("file:////home/rodrir23/data-test/output/");
		
		Configuration configuration = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "MAP REDUCE WITH CUSTOM INPUT FORMAT");

		job.setInputFormatClass(XMLFormat.class);//Setting my custom format
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputDataset);
		FileOutputFormat.setOutputPath(job, outputDir);
		outputDir.getFileSystem(job.getConfiguration()).delete(outputDir, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);			
	}
	
}
