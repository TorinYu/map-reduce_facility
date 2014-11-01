/**
 * 
 */
package mr;

import java.util.concurrent.Future;

import mr.Type.JOB_STATUS;
import mr.Type.MESSAGE_TYPE;
import mr.Type.TASK_TYPE;

/**
 * @author Nicolas_Yu
 *
 */
public class Message {
	private MESSAGE_TYPE messageType = null;
    private TASK_TYPE taskType = null;
    private Object content = null;
    private String jobId = null; 
    private String taskId = null;
    private JOB_STATUS jobStat = null;
    private TASK_TYPE taskStat = null;
    //private String blk_fpath = null;
    private String hostId = null;
    private Integer aval_procs = -1;
    private Future future = null;
    private String output_path = null;
}
