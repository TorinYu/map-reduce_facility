/**
 * 
 */
package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

import dfs.DataNode;
import dfs.NameNode;
import mr.Type.TASK_TYPE;
import mr.io.TextWritable;
import mr.io.Writable;

/**
 * @author Nicolas_Yu
 *
 */
public class Task implements Runnable{

	Class<? extends Mapper> mapper = null;
	Class<? extends Reducer> reducer = null;
	
	
	
	String jobId = null;
	String taskId = null;
	String hostId = null;
	String blockId = null; 
	int reduceNum = 0;
	String inputPath = null;
	String outputPath = null;
	String readFromHost = null;
	
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
			nameNode = (NameNode) hdfsRegistry.lookup("NameNode");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	/* 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (type == TASK_TYPE.Mapper) {
			try {
				Mapper<Object, Object, Object, Object> mapClass = mapper.newInstance();
				String outputPath = "/tmp" + jobId + "/" + hostId + '/';
				Context context = new Context(jobId, taskId, reduceNum, outputPath, TASK_TYPE.Mapper);
				// Waiting for JerrySun's API
				DataNode dataNode = nameNode.getDataNode(readFromHost);
				String content = dataNode.fetchBlock(blockId);
				String[] lines = content.split("\\n");
				for (int i = 0; i < lines.length; i++) {
					String line = lines[i];
					TextWritable key = new TextWritable();
					TextWritable value = new TextWritable();
					
					value.setVal(line);
					mapClass.map(key, value, context);
				}
				context.partionMapContent();
			} catch (InstantiationException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		} else {   //Reduce Task 
            Reducer<Object, Object, Object, Object> reducerClass = reducer.newInstance();
            Context context = new Context(jobId, taskId, reduceNum, outputPath, TASK_TYPE.Reducer);
            //String partition_id = task_id.split("_r_")[1].trim();
            String input_dir = "/tmp/"+jobId+'/'+hostId+'/';
            
            reducerClass.initialize(input_dir);
            reducerClass.mergePartition();

            ArrayList<RecordLine> reduceLines = reducerClass.getReduceLines();
            
            for (int i = 0; i < reduceLines.size(); i++) {
            	TextWritable key = (TextWritable) reduceLines.get(i).getKey();
            	Iterable<Writable> values = (Iterable<Writable>) reduceLines.get(i).getValue();
            	reducerClass.reduce(key, values, context);
            }
		}
		
	}


	public void setMapperClass(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}
	
	/**
	 * @param 
	 */
	public void setTaskType(TASK_TYPE type) {
		// TODO Auto-generated method stub
		
	}
	
	public void heartBeat() {
		
	}


	/**
	 * @return the blockId
	 */
	public String getBlockId() {
		return blockId;
	}


	/**
	 * @param blockId the blockId to set
	 */
	public void setBlockId(String blockId) {
		this.blockId = blockId;
	}


	/**
	 * @return the reduceNum
	 */
	public int getReduceNum() {
		return reduceNum;
	}


	/**
	 * @param reduceNum the reduceNum to set
	 */
	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}


	/**
	 * @return the input_path
	 */
	public String getInputPath() {
		return inputPath;
	}


	/**
	 * @param input_path the input_path to set
	 */
	public void setInputPath(String input_path) {
		this.inputPath = input_path;
	}


	/**
	 * @return the output_path
	 */
	public String getOutput_path() {
		return outputPath;
	}


	/**
	 * @param output_path the output_path to set
	 */
	public void setOutput_path(String output_path) {
		this.outputPath = output_path;
	}


	/**
	 * @return the readFromHost
	 */
	public String getReadFromHost() {
		return readFromHost;
	}


	/**
	 * @param readFromHost the readFromHost to set
	 */
	public void setReadFromHost(String readFromHost) {
		this.readFromHost = readFromHost;
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
	 * @param mapper the mapper to set
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
	 * @param reducer the reducer to set
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
	 * @param jobId the jobId to set
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
	 * @param taskId the taskId to set
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
	 * @param hostId the hostId to set
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
	 * @param outputPath the outputPath to set
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
	 * @param type the type to set
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
	 * @param nameNode the nameNode to set
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
	 * @param hdfsRegistry the hdfsRegistry to set
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
	 * @param readDir the readDir to set
	 */
	public void setReadDir(String readDir) {
		this.readDir = readDir;
	}
	
}
