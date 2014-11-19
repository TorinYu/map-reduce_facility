/**
 * 
 */
package mr;

import java.util.LinkedList;

import mr.io.Writable;

/**
 * @author Nicolas_Yu
 *
 */
public class RecordLine implements Comparable<RecordLine> {

	
	private Writable key;
	private Iterable<Writable> value;
	
	public RecordLine(Writable key) {
		this.key = key;
		this.value = new LinkedList<Writable>();
	}
	
	
	/* 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(RecordLine o) {
		return (int) Math.signum( Math.abs(this.key.getVal().hashCode()) - Math.abs(o.key.getVal().hashCode()));
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
	
	public void addValue(Writable value) {
		((LinkedList<Writable>)this.value).add(value);
	}

}
