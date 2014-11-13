package dfs;

import java.util.ArrayList;
import java.util.List;

public class FileInfo {

	private String fileName;
	private int replicas;
	private List<Integer> blockIds;

	public FileInfo(String fileName, int replicas) {
		this.fileName = fileName;
		this.blockIds = new ArrayList<Integer>();
		this.replicas = replicas;
	}

	public String getFileName() {
		return fileName;
	}

	public List<Integer> getBlockIds() {
		return blockIds;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getReplicas() {
		return replicas;
	}

	public void setReplicas(int replicas) {
		this.replicas = replicas;
	}

}
