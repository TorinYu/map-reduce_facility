package test;

import mr.Job;

public class WordCount {
	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		Job job = new Job(args[2], Integer.parseInt(args[3]));
		job.setInputFilePath(input);
		job.setFileName(input);
		job.setOutputFilePath(output);
		job.setMapperPath("test/WordCountMapper.class");
		job.setReducerPath("test/WordCountMapper.class");
		job.setMapper(WordCountMapper.class);
		job.setReducer(WordCountReducer.class);
		job.submit();
	}
}