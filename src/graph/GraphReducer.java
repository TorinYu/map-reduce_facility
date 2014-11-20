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
		String supporters = "";
		Iterator<Writable> it = values.iterator();
		while (it.hasNext()) {
			supporters += "," + (String) it.next().getVal();
		}
		supporters.replaceFirst(",", "[");
		supporters += "]";
		context.write(key, new TextWritable(supporters));
	}

}
