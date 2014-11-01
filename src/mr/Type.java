/**
 * 
 */
package mr;

/**
 * @author Nicolas_Yu
 *
 */
public class Type {
	public enum TASK_TYPE {
		Mapper, Reducer;
	}
	
	public enum JOB_STATUS {
		RUNNING, FINISHED, TERMINATED;
	}
	
	public enum MESSAGE_TYPE {
		REQ_BLK, REL_BLK, SET_JOB_STATUS, 
        START_MAPPER, TERMINATE_MAPPER, 
        HEARTBEAT;
	}
	
	public enum TASK_STATUS {
		RUNNING, FINISHED, TERMINATED;
	}
}
