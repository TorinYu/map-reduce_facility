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

	public static class WordCountMapper extends
			Mapper<TextWritable, TextWritable, TextWritable, IntWritable> {
		private static final long serialVersionUID = 1L;
		private final static IntWritable one = new IntWritable(1);
		private TextWritable word = new TextWritable();

		public void map(IntWritable key, TextWritable value, Context context) {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.setVal(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer extends
			Reducer<TextWritable, IntWritable, TextWritable, IntWritable> {

		private static final long serialVersionUID = 1L;

		public void reduce(TextWritable key, Iterable<Writable> values,
				Context context) {
			int sum = 0;
			IntWritable value = new IntWritable(1);
			for (Writable v : values) {
				TextWritable tw = (TextWritable) v;
				sum += Integer.parseInt(tw.getVal());
			}
			value.setVal(sum);
			context.write(key, value);
		}
	}

	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		Job job = new Job(args[2], Integer.parseInt(args[3]));
		job.setInputFilePath(input);
		job.setOutputFilePath(output);

		job.setMapper(WordCountMapper.class);
		job.setReducer(WordCountReducer.class);
		job.submit();
	}
}