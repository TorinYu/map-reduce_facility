package graph;

import java.util.Iterator;

import mr.Context;
import mr.Reducer;
import mr.io.IntWritable;
import mr.io.TextWritable;
import mr.io.Writable;

public class GraphReducer extends
		Reducer<TextWritable, IntWritable, TextWritable, IntWritable> {

	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(TextWritable key, Iterable<Writable> values,
			Context context) {
		Iterator<Writable> it = values.iterator();
		String supporters = "[" + (String) it.next().getVal();

		while (it.hasNext()) {
			supporters += "," + (String) it.next().getVal();
		}
		supporters += "]";
		context.write(key, new TextWritable(supporters));
	}

}
