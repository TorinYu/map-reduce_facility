/**
 * 
 */
package mr.io;

import java.io.Serializable;

/**
 * @author Nicolas_Yu
 * 
 */
public class IntWritable implements Serializable, Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7593209358292388107L;

	private Integer val = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.io.Writable#getVal()
	 */
	@Override
	public Integer getVal() {
		return val;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.io.Writable#setVal(java.lang.Object)
	 */
	@Override
	public void setVal(Object val) {
		this.val = (Integer) val;
	}

	public IntWritable(int val) {
		this.val = new Integer(val);
	}
}
