/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * @author Nicolas_Yu
 *
 */
public interface TaskTracker extends Serializable, Remote{
	
	public void setReducerNum(int reduceNum) throws RemoteException;
	
	public void heartBeat() throws RemoteException;

	/**
	 * read content from file
	 * 
	 * @param path
	 * @param name
	 * @return
	 */
	String readStr(String path, String name) throws RemoteException;

	/**
	 * write content to file
	 * 
	 * @param path
	 * @param content
	 */
	void writeFile(String path, byte[] content) throws RemoteException;

	/**
	 * read hashID files 
	 * 
	 * @param path
	 * @param hashID
	 * @return
	 */
	List<String> readDir(String path, String hashID) throws RemoteException;

	/**
	 * Write content to file
	 * 
	 * @param path
	 * @param content
	 */
	void writeStr(String path, String content) throws RemoteException;
	
    /**
     * terminate a task
     * @param taskID ID of the task
     */
    void terminate(String taskID) throws RemoteException;

	/**
	 * Terminate a taskTracker itself
	 * 
	 * @throws RemoteException
	 */
	void terminateSelf() throws RemoteException;

	/**
	 * Start reducer task
	 * 
	 * @param jobId
	 * @param reducerId
	 * @param writePath
	 * @param reducer
	 * @param clspath
	 */
	void startReducer(String jobId, String reducerId, String writePath,
			Class<? extends Reducer> reducer, String clspath) throws RemoteException;

	/**
	 * Start mapper task
	 * 
	 * @param jobId
	 * @param mapId
	 * @param blockId
	 * @param readFromHost
	 * @param mapper
	 * @param maperPath
	 */
	void startMapper(String jobId, String mapId, String blockId,
			String readFromHost, Class<? extends Mapper> mapper,
			String maperPath) throws RemoteException;

}
