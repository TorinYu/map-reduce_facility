package dfs;

import java.io.Serializable;
import java.util.List;

public class FileMeta implements Serializable {
	private static final long serialVersionUID = 1L;
	private String fileName;
	private List<Integer> replicaIds;
	private int blocks;

	public int getBlocks() {
		return blocks;
	}

	public void setBlocks(int blocks) {
		this.blocks = blocks;
	}

	public FileMeta(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public List<Integer> getReplicaIds() {
		return replicaIds;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setReplicaIds(List<Integer> replicaIds) {
		this.replicaIds = replicaIds;
	}
}
