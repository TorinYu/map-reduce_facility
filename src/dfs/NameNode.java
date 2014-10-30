package dfs;

import java.rmi.Remote;

/**
 * Name Node is in charge of storing data node's information. And also
 * responsible for communication when trying to uploading/ downloading files.
 * 
 * @author Jerry
 * 
 */
public interface NameNode extends Remote {

	/**
	 * Increment the id by 1, use it as key in the Registry. And return the id
	 * to the data node.
	 * 
	 * @param node
	 * @return generated data node id or -1 if there is an exception happens.
	 */
	public int registerDataNode(DataNode node);

	/**
	 * 
	 * @param fileName
	 * @return true if the update succeeds or false if it fails
	 */
	public void uploadFile(String fileName, String alias);

	/**
	 * 
	 * @param fileName
	 * @return a data node that stores the required block on that data node.
	 */
	public DataNode searchDataNode(String fileName, int blockId);

	/**
	 * Use this function to download files back.
	 * 
	 * @param fileName
	 * @return
	 */
	public String downloadFile(String fileName);

	/**
	 * Stop specific data node
	 * 
	 * @param datanode
	 * @return
	 */
	public void terminate(int datanode);

	/**
	 * Stop the name node and all data node that registers here.
	 * 
	 * @return boolean w
	 */
	public void terminate();

}
