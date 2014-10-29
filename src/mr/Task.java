/**
 * 
 */
package mr;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mr.Type.TASK_TYPE;

/**
 * @author Nicolas_Yu
 *
 */
public class Task implements Runnable{

	Class<? extends Mapper> mapper = null;
	Class<? extends Reducer> reducer = null;
	
	String job_id = null;
	String task_id = null;
	String host_id = null;
	String block_id = null;
	int reduceCount = 0;
	String input_path = null;
	String output_path = null;
	String readFromHost = null;
	
	
	Type.TASK_TYPE taskType = null;
	Registry hdfs_registry = null;
	
	
	public Task(String job_id, String task_id, String host, int port) {
		this.job_id = job_id;
		this.task_id = task_id;
		try {
			hdfs_registry = LocateRegistry.getRegistry(host, port);
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	/* 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}


	/**
	 * @param mapper2
	 */
	public void set_taskTP(TASK_TYPE mapper2) {
		// TODO Auto-generated method stub
		
	}
	
}
