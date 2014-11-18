/**
 * 
 */
package mr;

import java.io.Serializable;

/**
 * @author Nicolas_Yu
 * 
 */
public abstract class Mapper<K1, V1, K2, V2> implements Serializable {
	private static final long serialVersionUID = 1L;

	public abstract void map(K1 k1, V1 v1, Context context);

}
