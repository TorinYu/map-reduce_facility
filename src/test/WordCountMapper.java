package test;

import java.util.StringTokenizer;

import mr.Context;
import mr.Mapper;
import mr.io.IntWritable;
import mr.io.TextWritable;

public class WordCountMapper extends
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
