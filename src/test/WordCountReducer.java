package test;

import mr.Context;
import mr.Reducer;
import mr.io.IntWritable;
import mr.io.TextWritable;
import mr.io.Writable;

public class WordCountReducer extends
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
