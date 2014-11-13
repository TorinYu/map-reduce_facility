/**
 * 
 */
package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.Executors;

/**
 * @author Nicolas_Yu
 *
 */
public class TaskTrackerImpl implements TaskTracker, Runnable{

	String taskId = null;
	Registry mrRegistry = null;
	JobTracker jobTracker = null;
	int reduceNum = 0;

	private static final long serialVersionUID = 1L;

	public TaskTrackerImpl(String mrRegistryHost, int mrPort, int hdfsPort, int taskId, int reduceNum) {
		try {
			mrRegistry = LocateRegistry.getRegistry(mrRegistryHost, mrPort);
			jobTracker = (JobTracker) mrRegistry.lookup("JobTracker");
			this.reduceNum = reduceNum;
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* (non-Javadoc)
	 * @see mr.TaskTracker#setReduceNum()
	 */
	@Override
	public void setReduceNum() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#startMapper()
	 */
	@Override
	public void startMapper(String job_id, String reducer_id, String write_path, Class<? extends Reducer> reducer, String clspath)
    {
//        ClassDownloader dn = new ClassDownloader(clspath, clspath, registryHost, dfsPort);
//        try {
//            dn.download();
//        } catch (RemoteException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (NotBoundException e) {
//            e.printStackTrace();
//        }
        Task task = new Task(job_id, reducer_id, registryHost, dfsPort);
        task.set_taskTP(Type.TASK_TYPE.Mapper);
        task.set_reducer_cls(reducer);
        task.set_outputdir(write_path);
        task.set_reducerCT(reducer_ct);
        task.set_machineID(String.valueOf(id));
        Future f1 = exec.submit(task);
        taskID_exec.put(reducer_id, f1);


        Msg msg = new Msg();
        msg.setJob_id(job_id);
        msg.setTask_id(reducer_id);
        msg.setTask_tp(TASK_TP.REDUCER);
        msg.setTask_stat(TASK_STATUS.RUNNING);
        msg.setMachine_id(String.valueOf(id));
        msg.setOutput_path(write_path);
        msg.set_future(f1);
        this.heartbeats.offer(msg);
    }


	/* (non-Javadoc)
	 * @see mr.TaskTracker#startReducer()
	 */
	@Override
	public void startReducer() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#writeFile(java.lang.String, java.lang.String)
	 */
	@Override
	public void writeFile(String path, String content) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#readFile(java.lang.String, java.lang.String)
	 */
	@Override
	public String readFile(String path, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#sendHeartbeat()
	 */
	@Override
	public void sendHeartbeat() {
		// TODO Auto-generated method stub
		
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startMapper() {
		// TODO Auto-generated method stub
		
	}
	
	 
}
