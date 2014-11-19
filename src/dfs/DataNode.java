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
	 * @throws RemoteException
	 */
	public void createBlock(int blockId, String content) throws RemoteException;

	/**
	 * 
	 * @param blockId
	 * @param content
	 * @throws RemoteException
	 */
	public void createBlock(int blockId, byte[] content) throws RemoteException;

	/**
	 * create data node with the content it gets from another node.
	 * 
	 * @param blockId
	 * @param dataNode
	 * @throws RemoteException
	 */
	public void createBlock(int blockId, DataNode dataNode)
			throws RemoteException;

	/**
	 * Return byte array representation.
	 * 
	 * @param blockID
	 * @return a byte array representation
	 * @throws RemoteException
	 */
	public byte[] fetchByteBlock(int blockId) throws RemoteException;

	/**
	 * 
	 * @param blockId
	 * @return string representation of the block.
	 * @throws RemoteException
	 */
	public String fetchStringBlock(int blockId) throws RemoteException;

	/**
	 * terminate the data nodes.
	 * 
	 * @throws RemoteException
	 */
	public void terminate() throws RemoteException;

	/**
	 * Heart Beat Used for health check.
	 * 
	 * @throws RemoteException
	 */
	public void heartBeat() throws RemoteException;

	/**
	 * 
	 * @return id of the data node
	 * @throws RemoteException
	 */
	public int getId() throws RemoteException;

	/**
	 * set the id of the
	 * 
	 * @param id
	 *            to be set
	 * @throws RemoteException
	 */
	public void setId(int id) throws RemoteException;

	/**
	 * tells where the directory stores the files
	 * 
	 * @return folder of the blocks
	 * @throws RemoteException
	 */
	public String getFolder() throws RemoteException;

}
