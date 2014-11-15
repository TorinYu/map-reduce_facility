package dfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used record the information about blocks.
 * 
 * @author Jerry
 * 
 */
public class BlockInfo implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int blockId;
	private List<Integer> dataNodeIds;

	public BlockInfo(int blockId, String filename) {
		super();
		this.setBlockId(blockId);
		this.setDataNodeIds(new ArrayList<Integer>());
	}

	public int getBlockId() {
		return blockId;
	}

	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}

	public List<Integer> getDataNodeIds() {
		return dataNodeIds;
	}

	public void setDataNodeIds(List<Integer> dataNodeIds) {
		this.dataNodeIds = dataNodeIds;
	}

	@Override
	public String toString() {
		return this.blockId + "\t" + this.dataNodeIds.toString();
	}
}
