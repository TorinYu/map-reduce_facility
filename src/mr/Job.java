/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.UUID;

import mr.Type.JOB_STATUS;
import dfs.NameNode;

/**
 * @author Nicolas_Yu
 *
 */
public class Job implements Serializable{

	private static final long serialVersionUID = -1452869710596489185L;
	
	Class<? extends Mapper> mapper = null;
	Class<? extends Reducer> reducer = null;
	
	String mapperPath = null;
	String reducerPath = null;
	
	NameNode nameNode = null;
	
	Registry registry = null;
	
	
	String inputFilePath = null;
	String outputFilePath = null;
	
	JobTracker jobTracker = null;
	
	String jobId = null;
	
	int mapNum = 0;
	int reduceNum = 0; 
	
	int map_ct = 0;
	int reduce_ct = 0;
	
	JOB_STATUS jobStatus = null;
	
	
	public Job(String host, int port) {
		try {
			registry = LocateRegistry.getRegistry(host, port);
			jobTracker = (JobTracker) registry.lookup("JobTracker");
			Long uuid = Math.abs(UUID.randomUUID().getMostSignificantBits());
            jobId = String.valueOf(uuid);
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	public void submit(){
		jobTracker.schedule(this);
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
	 * @return the mapperPath
	 */
	public String getMapperPath() {
		return mapperPath;
	}


	/**
	 * @param mapperPath the mapperPath to set
	 */
	public void setMapperPath(String mapperPath) {
		this.mapperPath = mapperPath;
	}


	/**
	 * @return the reducerPath
	 */
	public String getReducerPath() {
		return reducerPath;
	}


	/**
	 * @param reducerPath the reducerPath to set
	 */
	public void setReducerPath(String reducerPath) {
		this.reducerPath = reducerPath;
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
	 * @return the registry
	 */
	public Registry getRegistry() {
		return registry;
	}


	/**
	 * @param registry the registry to set
	 */
	public void setRegistry(Registry registry) {
		this.registry = registry;
	}


	/**
	 * @return the inputFilePath
	 */
	public String getInputFilePath() {
		return inputFilePath;
	}


	/**
	 * @param inputFilePath the inputFilePath to set
	 */
	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}


	/**
	 * @return the outputFilePath
	 */
	public String getOutputFilePath() {
		return outputFilePath;
	}


	/**
	 * @param outputFilePath the outputFilePath to set
	 */
	public void setOutputFilePath(String outputFilePath) {
		this.outputFilePath = outputFilePath;
	}


	/**
	 * @return the jobTracker
	 */
	public JobTracker getJobTracker() {
		return jobTracker;
	}


	/**
	 * @param jobTracker the jobTracker to set
	 */
	public void setJobTracker(JobTracker jobTracker) {
		this.jobTracker = jobTracker;
	}


	/**
	 * @return the mapNum
	 */
	public int getMapNum() {
		return mapNum;
	}


	/**
	 * @param mapNum the mapNum to set
	 */
	public void setMapNum(int mapNum) {
		this.mapNum = mapNum;
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
	 * @return the map_ct
	 */
	public int getMap_ct() {
		return map_ct;
	}


	/**
	 * @param map_ct the map_ct to set
	 */
	public void setMap_ct(int map_ct) {
		this.map_ct = map_ct;
	}


	/**
	 * @return the reduce_ct
	 */
	public int getReduce_ct() {
		return reduce_ct;
	}


	/**
	 * @param reduce_ct the reduce_ct to set
	 */
	public void setReduce_ct(int reduce_ct) {
		this.reduce_ct = reduce_ct;
	}


	/**
	 * @return the jobStatus
	 */
	public JOB_STATUS getJobStatus() {
		return jobStatus;
	}


	/**
	 * @param jobStatus the jobStatus to set
	 */
	public void setJobStatus(JOB_STATUS jobStatus) {
		this.jobStatus = jobStatus;
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
}
