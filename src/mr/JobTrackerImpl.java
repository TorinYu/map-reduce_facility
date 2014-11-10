/**
 * 
 */
package mr;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


import mr.Type.JOB_STATUS;
import dfs.NameNode;

/**
 * @author Nicolas_Yu
 *
 */
//https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-6/failures

//The JobTracker is the service within Hadoop that farms out MapReduce tasks to specific nodes in the cluster, ideally the nodes that have the data, or at least are in the same rack.
//
//Client applications submit jobs to the Job tracker.
//The JobTracker talks to the NameNode to determine the location of the data
//The JobTracker locates TaskTracker nodes with available slots at or near the data
//The JobTracker submits the work to the chosen TaskTracker nodes.
//The TaskTracker nodes are monitored. If they do not submit heartbeat signals often enough, they are deemed to have failed and the work is scheduled on a different TaskTracker.
//A TaskTracker will notify the JobTracker when a task fails. The JobTracker decides what to do then: it may resubmit the job elsewhere, it may mark that specific record as something to avoid, and it may may even blacklist the TaskTracker as unreliable.
//When the work is completed, the JobTracker updates its status.
//Client applications can poll the JobTracker for information.
//The JobTracker is a point of failure for the Hadoop MapReduce service. If it goes down, all running jobs are halted.
//

public class JobTrackerImpl implements JobTracker, Runnable{
	
	String hdfsRegistryHost = null;
	int hdfsPort = 0;
	Registry hdfsRegistry = null;
	NameNode nameNode = null;
	
	String jobId = null;
	Registry mapReduceRegistry = null;
	int mapReducePort = 0;
	
	int jobTrackerPort = 0;
	JOB_STATUS jobStatus = null;
	int reduceNum = 0;
	
	ExecutorService executorService = null;
	
	HashMap<String, TaskTracker> registeredTaskTrackers = new HashMap<String, TaskTracker>();
	
	HashMap<String, HashMap<String, String>> jobMapperHost = new HashMap<String, HashMap<String, String>>();
	HashMap<String, HashMap<String, String>> jobReducerHost = new HashMap<String, HashMap<String, String>> ();
	
	/* Initilize the JobTracker
	 * 
	 */
	@Override
	public void initialize(String hdfsRegistryHost, int hdfsPort, int mapReducePort, int jobTrackerPort, int reduceNum) {
		
		
		try {
			this.mapReducePort = mapReducePort;
			mapReduceRegistry = LocateRegistry.createRegistry(this.mapReducePort);
			this.hdfsRegistryHost = hdfsRegistryHost;
			this.hdfsPort = hdfsPort;
			hdfsRegistry = LocateRegistry.getRegistry(hdfsRegistryHost, hdfsPort);
			this.nameNode = (NameNode) this.hdfsRegistry.lookup("NameNode");
			
			this.jobTrackerPort = jobTrackerPort;
			JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(this, this.jobTrackerPort);
			this.reduceNum = reduceNum;
			
			mapReduceRegistry.bind("JobTracler", stub); // TODO check with bind and rebind
			
			executorService = Executors.newCachedThreadPool();  // TODO 
			executorService.submit(this);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#schedule(mr.Job)
	 */
	@Override
	public void schedule(Job job) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#sendHeartbeat()
	 */
	@Override
	public void sendHeartbeat() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#describeJobs()
	 */
	@Override
	public void describeJobs() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#describeJob(mr.Job)
	 */
	@Override
	public void describeJob(Job job) {
		// TODO Auto-generated method stub
		
	}

	/* TaskTracker registers itself to JobTracker
	 * 
	 */
	@Override
	public void register(String hostId, TaskTracker taskTracker) {
		this.registeredTaskTrackers.put(hostId, taskTracker);
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#terminate()
	 */
	@Override
	public void terminate() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#kill(java.lang.String)
	 */
	@Override
	public void kill(String jobId) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#initialize()
	 */

	/* Check the aliveness of the registeredMchine by using heatBeat
	 * 
	 */
	@Override
	public void healthCheck() {
		while (true) {
			for (String hostId : registeredTaskTrackers.keySet()) {
				TaskTracker taskTracker = registeredTaskTrackers.get(hostId);
				try {
					taskTracker.heartBeat();
				} catch (RemoteException e) {
					System.out.println();
					registeredTaskTrackers.remove(hostId);
				}
				
			}
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	/* (non-Javadoc)
	 * @see mr.JobTracker#allocateMapper(java.lang.String, java.lang.String, java.lang.String, java.lang.String, mr.Job, com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable)
	 */
	@Override
	public void allocateMapper(String hostId, String mapId, String blockId,
			String readFromHost, Job job, HashMap<String,String> hostMapper) {
		TaskTracker taskTracker;
        try {
            taskTracker = this.registeredTaskTrackers.get(hostId);
            taskTracker.setReduceNum(reduceNum);
            hostMapper.put(hostId, mapId);
            jobMapperHost.put(job.getJobId(), hostMapper);
            System.out.println("prepare to start mapper");
            
            taskTracker.start_map(job.get_jobId(), mapper_id, blockID, read_from_machine, job.get_mapper_cls(), job.get_mapper_clspath());
            job.set_mapperStatus(mapper_id, TASK_STATUS.RUNNING);
            job.inc_mapperct();
            jobID_Job.put(job.get_jobId(), job);
            System.out.println("Job Tracker trying to start map task on "+String.valueOf(machineID)
                                                                            +", mapperID:"+mapper_id);
        } catch (AccessException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
		
	}
	
	
	
}
