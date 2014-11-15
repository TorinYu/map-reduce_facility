package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNode extends Remote {

	/**
	 * The file is trying to put the certain content on file. Using String to
	 * split data by line.
	 * 
	 * 
	 * @param blockId
	 * @param content
	 */
	public void createBlock(int blockId, String content);

	/**
	 * 
	 * @param blockId
	 * @param content
	 */
	public void createBlock(int blockId, byte[] content);

	/**
	 * create data node with the content it gets from another node.
	 * 
	 * @param blockId
	 * @param dataNode
	 */
	public void createBlock(int blockId, DataNode dataNode);

	/**
	 * Return byte array representation.
	 * 
	 * @param blockID
	 * @return a byte array representation
	 */
	public byte[] fetchByteBlock(int blockId);

	/**
	 * 
	 * @param blockId
	 * @return string representation of the block.
	 */
	public String fetchStringBlock(int blockId);

	/**
	 * terminate the data nodes.
	 */
	public void terminate();

	/**
	 * Heart Beat Used for health check.
	 */
	public void heartBeat() throws RemoteException;

	/**
	 * 
	 * @return id of the data node
	 */
	public int getId();

	/**
	 * set the id of the
	 * @param id to be set
	 */
	public void setId(int id);
	
	/**
	 * tells where the directory stores the files
	 * 
	 * @return folder of the blocks
	 */
	public String getFolder();

}
