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
    private int avalProcs = 0;
    
    
	/**
	 * @return the messageType
	 */
	public MESSAGE_TYPE getMessageType() {
		return messageType;
	}
	/**
	 * @param messageType the messageType to set
	 */
	public void setMessageType(MESSAGE_TYPE messageType) {
		this.messageType = messageType;
	}
	/**
	 * @return the taskType
	 */
	public TASK_TYPE getTaskType() {
		return taskType;
	}
	/**
	 * @param taskType the taskType to set
	 */
	public void setTaskType(TASK_TYPE taskType) {
		this.taskType = taskType;
	}
	/**
	 * @return the content
	 */
	public Object getContent() {
		return content;
	}
	/**
	 * @param content the content to set
	 */
	public void setContent(Object content) {
		this.content = content;
	}
	/**
	 * @return the jobId
	 */
	public String getJobId() {
		return jobId;
	}
	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	/**
	 * @return the taskId
	 */
	public String getTaskId() {
		return taskId;
	}
	/**
	 * @param taskId the taskId to set
	 */
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	/**
	 * @return the jobStat
	 */
	public JOB_STATUS getJobStat() {
		return jobStat;
	}
	/**
	 * @param jobStat the jobStat to set
	 */
	public void setJobStat(JOB_STATUS jobStat) {
		this.jobStat = jobStat;
	}
	/**
	 * @return the taskStat
	 */
	public TASK_TYPE getTaskStat() {
		return taskStat;
	}
	/**
	 * @param taskStat the taskStat to set
	 */
	public void setTaskStat(TASK_TYPE taskStat) {
		this.taskStat = taskStat;
	}
	/**
	 * @return the hostId
	 */
	public String getHostId() {
		return hostId;
	}
	/**
	 * @param hostId the hostId to set
	 */
	public void setHostId(String hostId) {
		this.hostId = hostId;
	}
	/**
	 * @return the aval_procs
	 */
	public Integer getAval_procs() {
		return aval_procs;
	}
	/**
	 * @param aval_procs the aval_procs to set
	 */
	public void setAval_procs(Integer aval_procs) {
		this.aval_procs = aval_procs;
	}
	/**
	 * @return the future
	 */
	public Future getFuture() {
		return future;
	}
	/**
	 * @param future the future to set
	 */
	public void setFuture(Future future) {
		this.future = future;
	}
	/**
	 * @return the output_path
	 */
	public String getOutput_path() {
		return output_path;
	}
	/**
	 * @param output_path the output_path to set
	 */
	public void setOutput_path(String output_path) {
		this.output_path = output_path;
	}
	/**
	 * @return the avalProcs
	 */
	public int getAvalProcs() {
		return avalProcs;
	}
	/**
	 * @param avalProcs the avalProcs to set
	 */
	public void setAvalProcs(int avalProcs) {
		this.avalProcs = avalProcs;
	}
}
