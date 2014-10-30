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
	 */
	public void createFileBlock(String filePath, String alias, BlockInfo block);

	/**
	 * @param fileName
	 * @param blockID
	 */
	public String fetchBlock(String fileName, int blockID);

	public void terminate();

}
