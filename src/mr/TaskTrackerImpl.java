/**
 * 
 */
package mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import dfs.ClassDownloader;
import mr.Type.MESSAGE_TYPE;
import mr.Type.TASK_STATUS;
import mr.Type.TASK_TYPE;


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
	
	boolean terminated = false;
	
	HashMap<String, Future> taskFuture = new HashMap<String, Future>();
	
	ExecutorService executorService = null;
	
	LinkedBlockingQueue<Message> heartbeats = new LinkedBlockingQueue<Message>();

	public TaskTrackerImpl(String mrRegistryHost, int mrPort, int hdfsPort, int selfPort, int taskId, int reduceNum, String inputDir) {
		try {
			
			
			this.mapReduceRegistryHost = mrRegistryHost;
			this.mapReducePort = mrPort;
			this.hdfsPort = hdfsPort;
			mapReduceRegistry = LocateRegistry.getRegistry(this.mapReduceRegistryHost, this.mapReducePort);
			this.inputDir = inputDir;
			jobTracker = (JobTracker) mapReduceRegistry.lookup("JobTracker");
			this.reduceNum = reduceNum;
			this.port = selfPort;
			
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
     * terminate a task
     * @param taskID ID of the task
     */
    public void terminate(String taskID)
    {
        Future f = taskFuture.get(taskID);
        ((java.util.concurrent.Future<?>) f).cancel(true);
        System.out.println("Task "+taskID+" terminated");
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
	            this.heartbeats.offer(initialMsg);
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
	public void startMapper(String jobId, String mapId, String blockId, String readFromHost, Class<? extends Mapper> mapper, String maperPath)
    {
        ClassDownloader dn = new ClassDownloader(maperPath, maperPath, hdfsRegistryHost, hdfsPort);
        try {
            dn.download();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        Task task = new Task(jobId, mapId, hdfsRegistryHost, hdfsPort);
        task.setTaskType(Type.TASK_TYPE.Mapper);
        task.setBlockId(blockId);
        task.setReduceNum(reduceNum);
        task.setMapper(mapper);
        task.setReadDir(inputDir);
        task.setHostId(hostId);
        task.setReadFromHost(readFromHost);
        Future f1 =  executorService.submit(task);
        taskFuture.put(mapId, f1);


        Message msg = new Message();
        msg.setJobId(jobId);
        msg.setTaskId(mapId);
        msg.setTaskType(TASK_TYPE.Mapper);
        msg.setTaskStat(TASK_STATUS.RUNNING);
        msg.setFuture(f1);
        msg.setHostId(String.valueOf(hostId));
        this.heartbeats.offer(msg);
    }


	/* (non-Javadoc)
	 * @see mr.TaskTracker#startReducer()
	 */
	@Override
	public void startReducer(String jobId, String reducerId, String writePath, Class<? extends Reducer> reducer, String clspath)
    {
        ClassDownloader dn = new ClassDownloader(clspath, clspath, registryHost, dfsPort);
        try {
            dn.download();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        Task task = new Task(jobId, reducerId, hdfsRegistryHost, hdfsPort);
        task.setTaskType(Type.TASK_TYPE.Reducer);
        task.setReducer(reducer);
        task.setOutput_path(writePath);
        task.setReduceNum(reduceNum);
        task.setHostId(hostId);
        Future f1 = (Future) executorService.submit(task);
        taskFuture.put(reducerId, f1);


        Message msg = new Message();
        msg.setJobId(jobId);
        msg.setTaskId(reducerId);
        msg.setTaskType(TASK_TYPE.Reducer);
        msg.setTaskStat(TASK_STATUS.RUNNING);
        msg.setHostId(String.valueOf(hostId));
        msg.setOutput_path(writePath);
        msg.setFuture(f1);
        this.heartbeats.offer(msg);
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
	
	@Override
	public void writeStr(String path, String content)
    {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(path, true));
            out.write(content+'\n');
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	 /**
     * read all partitioned filenames from the specified directory based on the hashedID
     * @param path DIR to read from
     * @param hashID partitions are hashed based on reducer numbers, 
     * this ID determines what partitions should be processed by a specific task
     */
	@Override
    public List<String> readDir(String path, String hashID)
    {
        File dir = new File(path);
        File[] files = dir.listFiles();
        List<String> ret_names = new ArrayList<String>();
        for (int i=0; i< files.length; i++)
        {
            String name = files[i].getName();
            String tmp[] = name.split("@");
            if (tmp.length > 0)
                if (tmp[tmp.length-1].equals(hashID))
                {
                    ret_names.add(name);        
                }
        }
        return ret_names;
    }
	
	/* Read String content from file
	 * 
	 */
	@Override
	public String readStr(String path, String name) {
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
	
	/**
     * mappers are asynchronous, this function checks if mapper is finished or terminated
     * @param msg
     */
    private void checkMapper(Message msg)
    {
        Future f = msg.getFuture();
        if (f != null)
        {
            if(f.isDone())
            {
                try {
                    if (f.get() == TASK_STATUS.TERMINATED)
                    {
                        msg.setTaskStat(TASK_STATUS.TERMINATED);
                        msg.setFuture(null);
                        jobTracker.checkHeartbeat(msg);
                    }
                    HashMap<String, Integer> idSize = (HashMap<String, Integer>) f.get();
                    msg.setFuture(null);
                    msg.setContent(idSize);
                    msg.setTaskStat(TASK_STATUS.FINISHED);
                    jobTracker.checkHeartbeat(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else if (f.isCancelled())
            {
                msg.setTaskStat(TASK_STATUS.TERMINATED);
                msg.setFuture(null);
                try {
                    jobTracker.checkHeartbeat(msg);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else
            {
                this.heartbeats.add(msg);
            }
        }
    }

    /**
     * reducers are asynchronous, this function checks if reducer is finished or terminated
     * @param msg
     */
    private void checkReducer(Message msg)
    {
        Future f1 = msg.getFuture();
        String reducer_id = msg.getTaskId();
        String output_path = msg.getOutput_path();
        if (f1 != null)
        {
            if (f1.isDone())
            {
                LinkedList<RecordLine> contents;
                try {
                    if (f1.get() != null)
                    {
                        if ( f1.get() == TASK_STATUS.TERMINATED)
                        {
                            msg.setTaskStat(TASK_STATUS.TERMINATED);
                            msg.setFuture(null);
                            jobTracker.checkHeartbeat(msg);
                        }
                    String path = output_path + '/' + reducer_id;
                    try {
                        FileUploader uploader = new FileUploader((String) f1.get(), path, 0, hdfsRegistryHost, hdfsPort);
                        uploader.upload();
                        System.out.println("output path:"+path);
                        System.out.println("reducerID:"+reducer_id);
                        System.out.println("Writing to DFS, REDUCER ID:"+reducer_id);
                    } catch (IOException e) {
                        // ignore
                    }
                    msg.setTaskStat(TASK_STATUS.FINISHED);
                    msg.setFuture(null);
                    jobTracker.checkHeartbeat(msg);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (NotBoundException e) {
                    e.printStackTrace();
                }
            }
            else if (f1.isCancelled())
            {
                msg.setTaskStat(TASK_STATUS.TERMINATED);
                try {
                    msg.setFuture(null);
                    jobTracker.checkHeartbeat(msg);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else
            {
                this.heartbeats.add(msg);
            }
        }
    }
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true)
        {
        	if (this.terminated)
        		return;
            Message msg = this.heartbeats.poll();
            if (msg != null)
            {
                if (msg.getTaskType() == TASK_TYPE.Mapper)
                    checkMapper(msg);
                else if (msg.getTaskType() == TASK_TYPE.Reducer)
                    checkReducer(msg);
				else
					try {
						jobTracker.checkHeartbeat(msg);
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            }
            else
            {
                try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
	}

	@Override
	public void startMapper() {
		// TODO Auto-generated method stub
		
	}
	
    
    /**
     * terminate this TaskTracker
     */
    @Override
    public void terminateSelf() throws RemoteException
    {
    	System.out.println("TaskTracker "+this.hostId + " is terminating");
    	this.terminated = true;
        UnicastRemoteObject.unexportObject(this, true);
        System.out.println("TaskTracker "+this.hostId + " terminated");
    }

	
	public static void main(String[] args) {
		/* TaskTracker ID should be the same with the id of the DataNode */
        String registryHost = args[0];
        int mrPort = Integer.valueOf(args[1]);
        int dfsPort = Integer.valueOf(args[2]);
        int selfPort = Integer.valueOf(args[3]);
        int id = Integer.valueOf(args[4]);
        String read_dir = args[5];
        int reducer_ct = Integer.valueOf(args[6]);
        TaskTrackerImpl tt = new TaskTrackerImpl(registryHost, mrPort, dfsPort, selfPort, id, reducer_ct, read_dir);
        tt.initialize();
	}

	@Override
	public void startReducer() {
		// TODO Auto-generated method stub
		
	}
	 
}
