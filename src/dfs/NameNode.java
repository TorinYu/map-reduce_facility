package dfs;

import java.io.File;
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
	public boolean uploadFile(String fileName);

	/**
	 * 
	 * @param fileName
	 * @return a file corresponding to the file name in the dfs.
	 */
	public File downloadFile(String fileName);

	/**
	 * Stop specific data node
	 * 
	 * @param datanode
	 * @return
	 */
	public boolean terminate(int datanode);

	/**
	 * Stop the name node and all data node that registers here.
	 * 
	 * @return
	 */
	public boolean terminate();

}
