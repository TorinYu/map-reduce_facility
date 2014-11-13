package dfs;

import java.rmi.Remote;

public interface DataNode extends Remote {

	/**
	 * This method is trying to take the command of DataNode to create file from
	 * file path. Since we are assuming the file is available at AFS, there is
	 * no need to consider locality.
	 * 
	 * @param filePath
	 * @param alias
	 * @param id
	 */
	public void createBlock(String filePath, String alias, int id);

	/**
	 * @param fileName
	 * @param blockID
	 * @return a string representation of the string it returns
	 */
	public String fetchBlock(String fileName, int blockID);

	/**
	 * Return byte array representation.
	 * 
	 * @param fileName
	 * @param blockID
	 * @return
	 */
	public byte[] fetchByteBlock(String fileName, int blockID);

	/**
	 * terminate the data nodes.
	 */
	public void terminate();

	/**
	 * Heart Beat Used for health check.
	 */
	public void heartBeat();

	/**
	 * 
	 * @return id of the data node
	 */
	public int getId();

	/**
	 * set the id of the
	 */
	public void setId(int id);

}
