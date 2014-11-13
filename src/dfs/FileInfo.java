package dfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileInfo implements Serializable {
	private static final long serialVersionUID = -2936352445461379648L;
	private String fileName;
	private ArrayList<BlockInfo> blocks;// make sure it is a sorted list.

	public FileInfo(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}

	public List<BlockInfo> getBlocks() {
		return blocks;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setBlocks(ArrayList<BlockInfo> blocks) {
		this.blocks = blocks;
	}

}
