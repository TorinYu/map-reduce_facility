package graph;

import mr.Context;
import mr.Mapper;
import mr.io.IntWritable;
import mr.io.TextWritable;

public class GraphMapper extends
		Mapper<TextWritable, TextWritable, TextWritable, IntWritable> {
	private static final long serialVersionUID = 1L;

	@Override
	public void map(TextWritable k1, TextWritable v1, Context context) {
		String line = v1.toString();
		String[] tokens = line.split("\\s+");
		context.write(new TextWritable(tokens[1]), new TextWritable(tokens[0]));
	}

}
