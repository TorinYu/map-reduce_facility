package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * Name Node is in charge of storing data node's information. And also
 * responsible for communication when trying to uploading/ downloading files.
 * 
 * @author Jerry Sun
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
	 * @return a FileInfo records the information of the file to be created.s
	 * @throws RemoteException
	 */
	public FileInfo createFile(String filename, int replicas)
			throws RemoteException;

	/**
	 * Retrieve the default block size
	 * 
	 * @return default block size
	 * @throws RemoteException
	 */
	public int getBlockSize() throws RemoteException;

	/**
	 * Commit the block allocation
	 * 
	 * @param dataNodeId
	 *            datanode id that sotres the block
	 * @param blockId
	 *            block id
	 * @throws RemoteException
	 */
	public void commitBlockAllocation(int dataNodeId, int blockId)
			throws RemoteException;

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
	 * next block Id to be used by the block, either it is data (part of file)
	 * or a whole runnable file
	 * 
	 * @return next block id
	 */
	public int getNextBlockId() throws RemoteException;

	/**
	 * The name node tries to allocate a data node stub to whomever wants this
	 * to get a data node to upload the files.
	 * 
	 * @param blockId
	 *            (the id of the block to be put on this node)
	 * @return an id representing the node
	 * @throws RemoteException
	 */
	public int allocateDataNode(int blockId) throws RemoteException;

	/**
	 * After the allocation of block, FileInfo now should contain the
	 * information for the specific file, now we should update the information
	 * to the remote side.
	 * 
	 * @param fileName
	 * @param fileInfo
	 */
	public void updateFileInfos(String fileName, FileInfo fileInfo);
	
	/**
	 * Check the health of all the data nodes. Mark failure ones as dead.
	 * @throws RemoteException 
	 * 
	 * 
	 */
	public void healthCheck() throws RemoteException;
	
}
