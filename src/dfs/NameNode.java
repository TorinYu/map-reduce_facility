package dfs;

import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * Name Node is in charge of storing data node's information. And also
 * responsible for communication when trying to uploading/ downloading files.
 * 
 * @author Jerry
 * 
 */
public interface NameNode extends Remote {

	/**
	 * Register a DataNode to this NameNode
	 * 
	 * 
	 * @param datanode
	 *            a stub of datanode
	 * @return id of the datanode
	 * @throws RemoteException
	 */
	public int register(DataNode node) throws RemoteException;

	/**
	 * Retrieve a stub of DataNode with given ID
	 * 
	 * @return a datanode instance
	 * @throws RemoteException
	 */
	public DataNode fetchDataNode(int id) throws RemoteException;

	/**
	 * Create metadata for given filename
	 * 
	 * @param filename
	 *            (file path and) filename
	 * @param replicas
	 *            requested replication factor
	 * @return actual replication factor
	 * @throws RemoteException
	 */
	public int createFile(String filename, int replicas) throws RemoteException;

	/**
	 * Retrieve the default block size
	 * 
	 * @return default block size
	 * @throws RemoteException
	 */
	public int getBlockSize() throws RemoteException;

	/**
	 * Retrieve the next DataNode for storing the replica
	 * 
	 * @return the next datanode chosen
	 * @throws RemoteException
	 */
	public DataNode allocateBlock() throws RemoteException;

	/**
	 * Commit the block allocation
	 * 
	 * @param dataNodeId
	 *            datanode id that sotres the block
	 * @param filename
	 *            filename of the block
	 * @param blockId
	 *            block id
	 * @throws RemoteException
	 */
	public void commitBlockAllocation(int dataNodeId, String filename,
			int blockId) throws RemoteException;

	/**
	 * Retrieve all block IDs with their DataNode IDs for the file with given
	 * filename
	 * 
	 * @param filename
	 *            filename of the blocks
	 * @return a map from block id to datanode ids
	 * @throws RemoteException
	 */

	public Map<Integer, List<Integer>> getAllBlocks(String filename)
			throws RemoteException;

	/**
	 * the status of DFS
	 * 
	 * @return status information of dfs
	 * @throws RemoteException
	 */
	public String dfsStatus() throws RemoteException;

	/**
	 * Terminate all nodes, and write metadata to fsImage
	 * 
	 * @param fsImageDir
	 *            directory to fsImage
	 * @throws RemoteException
	 */
	public void terminate() throws RemoteException;

	
	/**
	 * try to upload file via name node.
	 * 
	 * @param fileName
	 * 
	 */
	public void uploadFile(String fileName);

}
