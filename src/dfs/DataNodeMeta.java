package dfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class records the meta data for data node at the name node's side.
 * 
 * @author Jerry
 * 
 */
public class DataNodeMeta implements Comparable<DataNodeMeta>, Serializable {

	private static final long serialVersionUID = 1L;
	private int id;
	private boolean isLive;
	private List<Integer> blockIds;

	public DataNodeMeta(int id) {
		this.id = id;
		this.isLive = true;
		this.blockIds = new ArrayList<Integer>();
	}

	public boolean isLive() {
		return isLive;
	}

	public void setState(boolean state) {
		this.isLive = state;
	}

	public int getId() {
		return id;
	}

	public List<Integer> getBlockIds() {
		return blockIds;
	}

	@Override
	public int compareTo(DataNodeMeta o) {
		if (this.blockIds.size() == o.blockIds.size()) {
			return 0;
		} else if (this.blockIds.size() > o.blockIds.size()) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	public String toString() {
		return id + "\t" + this.blockIds.toString() + "\t"
				+ (this.isLive ? "Live" : "Dead");
	}
}
