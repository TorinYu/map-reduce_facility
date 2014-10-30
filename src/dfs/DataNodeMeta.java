package dfs;

import java.util.ArrayList;
import java.util.List;

/**
 * This class records the meta data for data node at the name node's side.
 * 
 * @author Jerry
 * 
 */
public class DataNodeMeta {
	private int id;
	private boolean isLive;
	private List<String> files;

	public DataNodeMeta(int id) {
		this.id = id;
		this.isLive = true;
		this.files = new ArrayList<String>();

	}

	public boolean isLive() {
		return isLive;
	}

	public List<String> getFiles() {
		return files;
	}

	public void setState(boolean state) {
		this.isLive = state;
	}

	public int getId() {
		return id;
	}

}
