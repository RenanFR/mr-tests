package hadoop.mr.tests.customformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class XMLReader extends RecordReader<LongWritable, Text>{

	private LineReader lineReader;//Auxiliary object to read file lines

	private final String tagStart = "<MOVIES>"; 
	private final String tagEnd = "</MOVIES>"; 
	
	//Those variables hold some positions to help navigate the file
	private long fileStart;
	private long fileEnd;
	private long currentPosition;
	
	//For each iteration while reading they will hold the key and value for mapping output
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	
	//Responsible to open the file and move the cursor to the begin
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)split;
		Configuration configuration = context.getConfiguration();
		fileStart = fileSplit.getStart();
		fileEnd = fileStart + fileSplit.getLength();
		Path file = fileSplit.getPath();
		FileSystem fileSystem = file.getFileSystem(configuration);
		FSDataInputStream inputStream = fileSystem.open(file);
		inputStream.seek(fileStart);
		lineReader = new LineReader(inputStream, configuration);
		this.currentPosition = fileStart;
		
	}

	//For each line of the file it will extract the key and value for mapping based on this logic
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key.set(currentPosition);
		value.clear();
		Text line = new Text();
		boolean foundStart = false;
		while (currentPosition < fileEnd) {
			long lineSize = lineReader.readLine(line);
			currentPosition = currentPosition + lineSize;
			if (!foundStart && line.toString().equalsIgnoreCase(this.tagStart)) {
				foundStart = true;
			} else if (foundStart && line.toString().equalsIgnoreCase(this.tagEnd)) {
				String withoutFinalComma = value.toString().substring(0, value.toString().length() - 1);
				value.set(withoutFinalComma);
				return true;
			} else if (foundStart) {
				String fullContent = line.toString();
				String content = fullContent.replaceAll("<[^>]+>", "");
				value.append(content.getBytes("UTF-8"), 0, content.length());
				value.append(",".getBytes("UTF-8"), 0, ",".length());
			}
		}
		return false;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	//Used to get the progress of the job
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (currentPosition - fileStart) / (float) (fileEnd - fileStart);
	}

	//Close external resources used in the process
	@Override
	public void close() throws IOException {
		if (lineReader != null) 
			lineReader.close();
	}

}
