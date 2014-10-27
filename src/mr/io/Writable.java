/**
 * 
 */
package mr.io;

import java.io.Serializable;

/**
 * an interface for all writable classes
 * @author Nicolas_Yu
 *
 */
public interface Writable extends Serializable{
	
	public Object getVal();
	
	public void setVal(Object val);
	
}
