package dfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BlockInfo {
	private int trunkNumber;
	private ArrayList<Integer> dataNodeIds;// make sure it is a sorted list.
	private int replicationNumber;

	public BlockInfo(int trunkNumber) {
		this.trunkNumber = trunkNumber;
	}

	public int getTrunkNumber() {
		return trunkNumber;
	}

	public void setTrunkNumber(int trunkNumber) {
		this.trunkNumber = trunkNumber;
	}

	public List<Integer> getDataNodeIds() {
		return dataNodeIds;
	}

	public void setDataNodeIds(ArrayList<Integer> dataNodeIds) {
		this.dataNodeIds = dataNodeIds;
		Collections.sort(this.dataNodeIds);
	}

	public int getReplicationNumber() {
		return replicationNumber;
	}

	public void setReplicationNumber(int replicationNumber) {
		this.replicationNumber = replicationNumber;
	}
}
