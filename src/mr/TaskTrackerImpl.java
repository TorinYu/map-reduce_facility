/**
 * 
 */
package mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import mr.Type.MESSAGE_TYPE;
import mr.Type.TASK_TYPE;

import com.sun.corba.se.impl.orbutil.closure.Future;

/**
 * @author Nicolas_Yu
 *
 */
public class TaskTrackerImpl implements TaskTracker, Runnable{

	private static final long serialVersionUID = 1L;
	
	String hdfsRegistryHost = null;
	int hdfsPort = 0;
	
	String hostId = null;
	int port = 0;
	
	String taskId = null;
	String mapReduceRegistryHost = null;
	int mapReducePort = 0;
	Registry mapReduceRegistry = null;
	
	String inputDir = null;
	
	JobTracker jobTracker = null;
	int reduceNum = 0;
	
	HashMap<String, Future> taskFuture = new HashMap<String, Future>();
	
	ExecutorService executorService = null;

	public TaskTrackerImpl(String mrRegistryHost, int mrPort, int hdfsPort, int taskId, int reduceNum, String inputDir) {
		try {
			
			
			this.mapReduceRegistryHost = mrRegistryHost;
			this.mapReducePort = mrPort;
			mapReduceRegistry = LocateRegistry.getRegistry(this.mapReduceRegistryHost, this.mapReducePort);
			this.inputDir = inputDir;
			jobTracker = (JobTracker) mapReduceRegistry.lookup("JobTracker");
			this.reduceNum = reduceNum;
			
			executorService = Executors.newCachedThreadPool();
			executorService.submit(this);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	 /**
	 * @param registryHost
	 * @param mrPort
	 * @param dfsPort
	 * @param selfPort
	 * @param id
	 * @param read_dir
	 * @param reducer_ct
	 */
	public TaskTrackerImpl(String registryHost, String mrPort, String dfsPort,
			String selfPort, int id, String read_dir, int reducer_ct) {
		// TODO Auto-generated constructor stub
	}

	public void initialize()
	    {
	        try {
	            TaskTracker stub = (TaskTracker) UnicastRemoteObject.exportObject(this, port);
	            jobTracker.register(hostId, stub);
	            
	            Message initialMsg = new Message();
	            Integer avalProcs = new Integer(Runtime.getRuntime().availableProcessors());
	            System.out.println("Avalible CPU numbers:"+avalProcs.toString());
	            initialMsg.setAval_procs(avalProcs);
	            initialMsg.setMessageType(MESSAGE_TYPE.HEARTBEAT);
	            initialMsg.setHostId(hostId);
	            //this.heartbeats.offer(hb_msg);
	          } catch (Exception e) {
	            e.printStackTrace();
	          }
	    }
	
	/* (non-Javadoc)
	 * @see mr.TaskTracker#setReduceNum()
	 */
	@Override
	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
		
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#startMapper()
	 */
	@Override
	public void startMapper(String job_id, String reducerId, String writePath, Class<? extends Reducer> reducer, String clspath)
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
        Task task = new Task(job_id, reducerId, hdfsRegistryHost, hdfsPort);
        task.setTaskType(Type.TASK_TYPE.Mapper);
        task.setReducer(reducer);
        task.setOutput_path(writePath);
        task.setReduceNum(reduceNum);
        task.setHostId(hostId);
        Future f1 = (Future) executorService.submit(task);
        taskFuture.put(reducerId, f1);


//        Msg msg = new Msg();
//        msg.setJob_id(job_id);
//        msg.setTask_id(reducer_id);
//        msg.setTask_tp(TASK_TP.REDUCER);
//        msg.setTask_stat(TASK_STATUS.RUNNING);
//        msg.setMachine_id(String.valueOf(id));
//        msg.setOutput_path(write_path);
//        msg.set_future(f1);
//        this.heartbeats.offer(msg);
    }


	/* (non-Javadoc)
	 * @see mr.TaskTracker#startReducer()
	 */
	@Override
	public void startReducer(String job_id, String reducerId, String writePath, Class<? extends Reducer> reducer, String clspath)
    {
        //ClassDownloader dn = new ClassDownloader(clspath, clspath, registryHost, dfsPort);
        try {
            dn.download();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        Task task = new Task(job_id, reducerId, hdfsRegistryHost, hdfsPort);
        task.setTaskType(Type.TASK_TYPE.Reducer);
        task.setReducer(reducer);
        task.setOutput_path(writePath);
        task.setReduceNum(reduceNum);
        task.setHostId(hostId);
        Future f1 = (Future) executorService.submit(task);
        taskFuture.put(reducerId, f1);


//        Msg msg = new Msg();
//        msg.setJob_id(job_id);
//        msg.setTask_id(reducer_id);
//        msg.setTask_tp(TASK_TP.REDUCER);
//        msg.setTask_stat(TASK_STATUS.RUNNING);
//        msg.setMachine_id(String.valueOf(id));
//        msg.setOutput_path(write_path);
//        msg.set_future(f1);
//        this.heartbeats.offer(msg);
    }

	/* Write content to file
	 * 
	 */
	@Override
	public void writeFile(String path, byte[] content) {
		FileOutputStream writeFile;
		try {
			writeFile = new FileOutputStream(path, true);
			writeFile.write(content);
			writeFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	/* Read String content from file
	 * 
	 */
	@Override
	public String readFile(String path, String name) {
		StringBuilder content = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(path+'/'+name));
            String line = null;
            while ((line = br.readLine()) != null)
                content.append(line+'\n');
            br.close();
            File delf = new File(path + '/' + name);
            delf.delete();
            return content.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content.toString();
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#sendHeartbeat()
	 */
	@Override
	public void heartBeat() {
		// TODO Auto-generated method stub
		
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String[] args) {
		/* TaskTracker ID should be the same with the id of the DataNode */
        String registryHost = args[0];
        String mrPort = args[1];
        String dfsPort = args[2];
        String selfPort = args[3];
        int id = Integer.valueOf(args[4]);
        String read_dir = args[5];
        int reducer_ct = Integer.valueOf(args[6]);
        TaskTrackerImpl tt = new TaskTrackerImpl(registryHost, mrPort, dfsPort, selfPort, id, read_dir, reducer_ct);
        tt.initialize();
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#startMapper()
	 */
	@Override
	public void startMapper() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.TaskTracker#startReducer()
	 */
	@Override
	public void startReducer() {
		// TODO Auto-generated method stub
		
	}
	
	 
}
