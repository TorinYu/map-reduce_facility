package test;

import java.util.StringTokenizer;
import mr.io.IntWritable;
import mr.io.TextWritable;
import mr.io.Writable;
import mr.Context;
import mr.Job;
import mr.Mapper;
import mr.Reducer;

public class WordCount {
	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		Job job = new Job(args[2], Integer.parseInt(args[3]));
		job.setInputFilePath(input);
		job.setFileName(input);
		job.setOutputFilePath(output);

		job.setMapper(WordCountMapper.class);
		job.setReducer(WordCountReducer.class);
		job.submit();
	}
}