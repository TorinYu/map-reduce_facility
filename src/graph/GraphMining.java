package graph;

import mr.Job;

public class GraphMining {
	public static void main(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		Job job = new Job(args[2], Integer.parseInt(args[3]));
		job.setInputFilePath(input);
		job.setFileName(input);
		job.setOutputFilePath(output);
		job.setMapperPath("graph/GraphMapper.class");
		job.setReducerPath("graph/GraphReducer.class");
		job.setMapper(GraphMapper.class);
		job.setReducer(GraphReducer.class);
		job.submit();
	}
}
