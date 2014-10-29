/**
 * 
 */
package mr;

import mr.io.Writable;

/**
 * @author Nicolas_Yu
 *
 */
public class RecordLine implements Comparable<RecordLine> {

	
	private Writable key;
	private Iterable<Writable> value;
	
	public RecordLine(Writable key, Iterable<Writable> value) {
		this.key = key;
		this.value = value;
	}
	
	
	/* 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(RecordLine o) {
		// TODO Auto-generated method stub
		return this.key.getVal().hashCode() - o.key.getVal().hashCode();
	}


	/**
	 * @return the key
	 */
	public Writable getKey() {
		return key;
	}


	/**
	 * @param key the key to set
	 */
	public void setKey(Writable key) {
		this.key = key;
	}


	/**
	 * @return the value
	 */
	public Iterable<Writable> getValue() {
		return value;
	}


	/**
	 * @param value the value to set
	 */
	public void setValue(Iterable<Writable> value) {
		this.value = value;
	}

}
