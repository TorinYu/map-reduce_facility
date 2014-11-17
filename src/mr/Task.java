/**
 * 
 */
package mr;

import java.io.File;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import dfs.DataNode;
import dfs.NameNode;
import mr.Type.TASK_TYPE;
import mr.io.IntWritable;
import mr.io.TextWritable;
import mr.io.Writable;

/**
 * @author Nicolas_Yu
 * 
 */
public class Task implements Callable<Object> {

	private static final String NAMENODE = "namenode";
	private Class<? extends Mapper> mapper = null;
	private Class<? extends Reducer> reducer = null;

	private String jobId = null;
	private String taskId = null;
	private String hostId = null;

	private int reducerNum = 0;
	private String inputPath = null;
	private String outputPath = null;
	private int blockId = 0;
	private int dataNodeId = 0;

	public int getDataNodeId() {
		return dataNodeId;
	}

	public void setDataNodeId(int dataNodeId) {
		this.dataNodeId = dataNodeId;
	}

	String readDir = null;

	Type.TASK_TYPE type = null;

	NameNode nameNode = null;

	Type.TASK_TYPE taskType = null;
	Registry hdfsRegistry = null;

	public Task(String job_id, String task_id, String host, int port) {
		this.jobId = job_id;
		this.taskId = task_id;
		try {
			hdfsRegistry = LocateRegistry.getRegistry(host, port);
			nameNode = (NameNode) hdfsRegistry.lookup(NAMENODE);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

	}

	/*
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public Object call() {
		// System.out.println(type.toString());
		if (Thread.interrupted()) {
			return Type.TASK_STATUS.TERMINATED;
		}
		if (type == TASK_TYPE.Mapper) {
			try {
				Mapper<TextWritable, TextWritable, TextWritable, IntWritable> mapClass = mapper
						.newInstance();
				String outputPath = "/tmp/" + jobId + "/" + hostId + '/';

				Context context = new Context(jobId, taskId, reducerNum,
						outputPath, TASK_TYPE.Mapper);
				

				DataNode dataNode = nameNode.fetchDataNode(dataNodeId);
				String content = dataNode.fetchStringBlock(blockId);
				// System.out.println("Content is " + content);
				String[] lines = content.split("\n");
				for (int i = 0; i < lines.length; i++) {
					// System.out.println("Line " + i + " is " + lines[i]);
					String line = lines[i];
					TextWritable key = new TextWritable();
					TextWritable value = new TextWritable();
					value.setVal(line);
					mapClass.map(key, value, context);
				}
				context.partionMapContent();

			} catch (InstantiationException | IllegalAccessException e) {
				e.printStackTrace();
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return Type.TASK_STATUS.FINISHED;
		} else { // Reducer Task
			Reducer<Writable, Writable, Writable, Writable> reducerClass;
			try {
				reducerClass = reducer.newInstance();
				Context context = new Context(jobId, taskId, reducerNum,
						outputPath, TASK_TYPE.Reducer);
				String inputDir = "/tmp/" + jobId + '/' + hostId + '/';
				System.out.println("Input Dir is " + inputDir);
				System.out.println("Output Dir is " + outputPath);
				

				reducerClass.initialize(inputDir);
				reducerClass.mergePartition();

				ArrayList<RecordLine> reduceLines = reducerClass
						.getReduceLines();

				for (int i = 0; i < reduceLines.size(); i++) {
					TextWritable key = (TextWritable) reduceLines.get(i)
							.getKey();
					Iterable<Writable> values = (Iterable<Writable>) reduceLines
							.get(i).getValue();
					reducerClass.reduce(key, values, context);
				}
				
				return inputDir;
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}

	}

	public void setMapperClass(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	/**
	 * @param
	 */
	public void setTaskType(TASK_TYPE type) {
		this.type = type;

	}

	public void heartBeat() {

	}

	/**
	 * @return the blockId
	 */
	public int getBlockId() {
		return blockId;
	}

	/**
	 * @param blockId
	 *            the blockId to set
	 */
	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}

	/**
	 * @return the reduceNum
	 */
	public int getReduceNum() {
		return reducerNum;
	}

	/**
	 * @param reduceNum
	 *            the reduceNum to set
	 */
	public void setReducerNum(int reduceNum) {
		this.reducerNum = reduceNum;
	}

	/**
	 * @return the input_path
	 */
	public String getInputPath() {
		return inputPath;
	}

	/**
	 * @param input_path
	 *            the input_path to set
	 */
	public void setInputPath(String input_path) {
		this.inputPath = input_path;
	}

	/**
	 * @return the taskType
	 */
	public Type.TASK_TYPE getTaskType() {
		return taskType;
	}

	/**
	 * @return the mapper
	 */
	public Class<? extends Mapper> getMapper() {
		return mapper;
	}

	/**
	 * @param mapper
	 *            the mapper to set
	 */
	public void setMapper(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	/**
	 * @return the reducer
	 */
	public Class<? extends Reducer> getReducer() {
		return reducer;
	}

	/**
	 * @param reducer
	 *            the reducer to set
	 */
	public void setReducer(Class<? extends Reducer> reducer) {
		this.reducer = reducer;
	}

	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}

	/**
	 * @param jobId
	 *            the jobId to set
	 */
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * @param taskId
	 *            the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	/**
	 * @return the hostId
	 */
	public String getHostId() {
		return hostId;
	}

	/**
	 * @param hostId
	 *            the hostId to set
	 */
	public void setHostId(String hostId) {
		this.hostId = hostId;
	}

	/**
	 * @return the outputPath
	 */
	public String getOutputPath() {
		return outputPath;
	}

	/**
	 * @param outputPath
	 *            the outputPath to set
	 */
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	/**
	 * @return the type
	 */
	public Type.TASK_TYPE getType() {
		return type;
	}

	/**
	 * @param type
	 *            the type to set
	 */
	public void setType(Type.TASK_TYPE type) {
		this.type = type;
	}

	/**
	 * @return the nameNode
	 */
	public NameNode getNameNode() {
		return nameNode;
	}

	/**
	 * @param nameNode
	 *            the nameNode to set
	 */
	public void setNameNode(NameNode nameNode) {
		this.nameNode = nameNode;
	}

	/**
	 * @return the hdfsRegistry
	 */
	public Registry getHdfsRegistry() {
		return hdfsRegistry;
	}

	/**
	 * @param hdfsRegistry
	 *            the hdfsRegistry to set
	 */
	public void setHdfsRegistry(Registry hdfsRegistry) {
		this.hdfsRegistry = hdfsRegistry;
	}

	/**
	 * @return the readDir
	 */
	public String getReadDir() {
		return readDir;
	}

	/**
	 * @param readDir
	 *            the readDir to set
	 */
	public void setReadDir(String readDir) {
		this.readDir = readDir;
	}

}
