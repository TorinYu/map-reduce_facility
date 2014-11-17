/**
 * 
 */
package mr.io;

/**
 * @author Nicolas_Yu
 *
 */
public class TextWritable implements Writable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6761529145767170724L;
	
	private String val = null;
	
	public TextWritable(Object val) {
		this.val = (String)val;
	}

	public TextWritable() {
	}

	/* (non-Javadoc)
	 * @see mr.io.Writable#getVal()
	 */
	@Override
	public String getVal() {
		return val;
	}

	/* (non-Javadoc)
	 * @see mr.io.Writable#setVal(java.lang.Object)
	 */
	@Override
	public void setVal(Object val) {
		this.val = (String)val;
	}
	
	@Override
	public String toString(){
		return val; 
	}
	
}
