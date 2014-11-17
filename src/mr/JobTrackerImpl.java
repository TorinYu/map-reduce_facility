/**
 * 
 */
package mr;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import mr.Type.JOB_STATUS;
import mr.Type.MESSAGE_TYPE;
import mr.Type.TASK_STATUS;
import mr.Type.TASK_TYPE;
import dfs.NameNode;

/**
 * @author Nicolas_Yu
 *
 */
//https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-6/failures

/**
 * 
 * The JobTracker is the service within Hadoop that farms out MapReduce tasks to
 * specific nodes in the cluster, ideally the nodes that have the data, or at
 * least are in the same rack.
 * 
 * Client applications submit jobs to the Job tracker. The JobTracker talks to
 * the NameNode to determine the location of the data The JobTracker locates
 * TaskTracker nodes with available slots at or near the data The JobTracker
 * submits the work to the chosen TaskTracker nodes. The TaskTracker nodes are
 * monitored. If they do not submit heartbeat signals often enough, they are
 * deemed to have failed and the work is scheduled on a different TaskTracker. A
 * TaskTracker will notify the JobTracker when a task fails. The JobTracker
 * decides what to do then: it may resubmit the job elsewhere, it may mark that
 * specific record as something to avoid, and it may may even blacklist the
 * TaskTracker as unreliable. When the work is completed, the JobTracker updates
 * its status. Client applications can poll the JobTracker for information. The
 * JobTracker is a point of failure for the Hadoop MapReduce service. If it goes
 * down, all running jobs are halted.
 */

public class JobTrackerImpl implements JobTracker, Runnable {

	private static final long serialVersionUID = 1L;
	private Registry hdfsRegistry = null;
	private NameNode nameNode = null;

	private String jobId = null;
	private Registry mapReduceRegistry = null;
	private int mapReducePort = 0;

	private int jobTrackerPort = 0;
	private JOB_STATUS jobStatus = null;
	private int reducerNum = 0;

	private boolean terminated = false;

	private ExecutorService executorService = null;

	// Information of active TaskTracker with hostId
	private Hashtable<String, TaskTracker> registeredTaskTrackers = new Hashtable<String, TaskTracker>();

	// Information of JobId and output File dir
	private Hashtable<String, String> jobId_OutputFileDir = new Hashtable<String, String>();

	// Information of hostId and available Slots
	private Hashtable<String, Integer> availableSlots = new Hashtable<String, Integer>();

	private Hashtable<String, Set<String>> runningJobs = new Hashtable<String, Set<String>>();

	// Information of Job and related MapId_hostId
	private Hashtable<String, Hashtable<String, String>> jobMapperHost = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, String>> jobReducerHost = new Hashtable<String, Hashtable<String, String>>();

	private Hashtable<String, Job> jobId_Job = new Hashtable<String, Job>();

	// Information of JobID, MachineID, partitionID, size
	HashMap<String, HashMap<String, HashMap<String, Integer>>> job_mc_hash_size = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();

	private static final String NAMENODE = "namenode";

	/**
	 * Initialize the JobTracker, create the MR Registry and bind the JobTracker
	 * 
	 * @param dfsHost
	 */
	@Override
	public void initialize(int mrPort, String hdfsRegistryHost, int hdfsPort,
			int jobTrackerPort, int reducerNum) throws RemoteException {

		try {
			this.mapReducePort = mrPort;
			mapReduceRegistry = LocateRegistry
					.createRegistry(this.mapReducePort);

			hdfsRegistry = LocateRegistry.getRegistry(hdfsRegistryHost,
					hdfsPort);
			this.nameNode = (NameNode) this.hdfsRegistry.lookup(NAMENODE);

			this.jobTrackerPort = jobTrackerPort;
			JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(
					this, this.jobTrackerPort);
			this.reducerNum = reducerNum;

			mapReduceRegistry.bind("JobTracker", stub);
			executorService = Executors.newCachedThreadPool();
			executorService.submit(this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.JobTracker#schedule(mr.Job)
	 */
	@Override
	public void schedule(Job job) throws RemoteException {
		try {
			Map<Integer, List<Integer>> mappings = this.nameNode
					.getAllBlocks(job.getFileName());
			job.setJobStatus(JOB_STATUS.RUNNING);
			jobId_Job.put(job.getJobId(), job);
			Set<?> set = mappings.entrySet();
			if (job.getOutputFilePath() != null)
				jobId_OutputFileDir
						.put(job.getJobId(), job.getOutputFilePath());
			System.out.println("Scheduling Job " + job.getJobId());
			Hashtable<String, String> mc_mp = new Hashtable<String, String>();
			for (Iterator<?> iter = set.iterator(); iter.hasNext();) {
				@SuppressWarnings("rawtypes")
				Entry entry = (Entry) iter.next();
				Integer blockID = (Integer) entry.getKey();
				@SuppressWarnings("unchecked")
				List<Integer> value = (List<Integer>) entry.getValue();
				System.out.println("BlockID:" + blockID.toString());
				/* set mapper task */
				String mapId = job.getJobId() + "_m_" + String.valueOf(blockID);
				boolean allocated = false;
				for (Integer hostId : value) {
					if (!this.registeredTaskTrackers.containsKey(String
							.valueOf(hostId)))
						continue;
					System.out.println("MachineID:" + hostId.toString());
					int availableSlots = this.availableSlots.get(String
							.valueOf(hostId));
					System.out.println("Aval Slots NUM:" + availableSlots);
					if (availableSlots > 0) {
						System.out.println("Availble CPU, machine: " + hostId);
						String readFromHost = String.valueOf(hostId);
						allocateMapper(String.valueOf(hostId), mapId,
								String.valueOf(blockID), readFromHost, job,
								mc_mp);
						availableSlots--;
						this.availableSlots.put(String.valueOf(hostId),
								availableSlots);
						allocated = true;
						Set<String> runningJobIDs = runningJobs.get(hostId);
						if (runningJobIDs == null) {
							runningJobIDs = new HashSet<String>();
							runningJobIDs.add(job.getJobId());
							runningJobs.put(String.valueOf(hostId),
									runningJobIDs);
						} else {
							runningJobIDs.add(job.getJobId());
						}
						break;
					}
				}
				if (!allocated) {
					String hostId = chooseSpareMachine();
					if (!hostId.equals("")) {
						int avaiSlots = this.availableSlots.get(hostId);
						String read_from_machine = String.valueOf(value.get(0));
						allocateMapper(hostId, mapId, String.valueOf(blockID),
								read_from_machine, job, mc_mp);
						avaiSlots--;
						this.availableSlots.put(hostId, avaiSlots);
						allocated = true;
						Set<String> runningJobIDs = runningJobs.get(hostId);
						if (runningJobIDs == null) {
							runningJobIDs = new HashSet<String>();
							runningJobIDs.add(job.getJobId());
							runningJobs.put(hostId, runningJobIDs);
						} else {
							runningJobIDs.add(job.getJobId());
						}
					} else {
						System.out.println("No Available Machine");
					}
				}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * chooseReducer: Sum the sizes of partitions given back by mapper tasks,
	 * Choose the Top N (N = num of reducers) sizes, for each partition,
	 * allocate reducers on the machine that has most resources.
	 * 
	 * @param jobID
	 *            job's id
	 * @return a hashmap of the assignment
	 * 
	 *         Eg. We have 2 reducers. Machine A has partition k1 = 1KB, k2 =
	 *         1MB Machine B has partition k1 = 1MB, k2 = 1GB Machine C has
	 *         partition k1 = 1GB, k2 = 1TB sum(k1) = 1GB + 1MB + 1KB sum(k2) =
	 *         1TB + 1GB + 1MB
	 * 
	 *         If schedule reducer on A, A has to pull 1MB+1GB, 1GB+1TB files If
	 *         schedule reducer on B, B has to pull 1KB+1GB, 1MB+1TB files If
	 *         schedule reducer on C, C has to pull 1KB+1MB, 1MB+1GB files
	 * 
	 *         C has the most resources for k1 and k2, Thus we choose to
	 *         schedule 2 reducers on C.
	 * */
	@Override
	public HashMap<String, List<String>> chooseReducer(String jobID) throws RemoteException {
		/* partition_res: mapping of machineID and hashedID */
		HashMap<String, List<String>> partition_res = new HashMap<String, List<String>>();
		HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size
				.get(jobID);
		/* allocate #reducer_ct reducers */
		for (int i = 0; i < reducerNum; i++) {
			TreeMap<Integer, String> priorityQ = new TreeMap<Integer, String>();
			/* insert each record into a priority queue */
			for (Entry<String, HashMap<String, Integer>> entry : mc_hash_size
					.entrySet()) {
				System.out.println("Entry:" + entry.toString());
				Integer size = entry.getValue().get(String.valueOf(i));
				if (size == null)
					size = 0;
				priorityQ.put(size, entry.getKey());
				System.out.println("PriorityQ:" + priorityQ.toString());
			}
			/*
			 * iteratively get the machine with most records, and check #cpu
			 * available
			 */
			Entry<Integer, String> entry = null;
			String machineId = "";
			boolean allocated = false;
			while ((entry = priorityQ.pollLastEntry()) != null) {
				machineId = entry.getValue();
				int availCPUs = availableSlots.get(machineId);
				if (availCPUs > 0) {
					availCPUs--;
					availableSlots.put(machineId, availCPUs);
					allocated = true;
					break;
				}
			}
			if (!allocated) {
				/* no idle machine found */
				String id = chooseSpareMachine();
				if (!id.equals("")) {
					machineId = id;
					int availCPUs = availableSlots.get(machineId);
					availCPUs--;
					availableSlots.put(machineId, availCPUs);
				}
			}
			List<String> lst = partition_res.get(machineId);
			if (lst == null)
				lst = new ArrayList<String>();
			lst.add(String.valueOf(i));
			partition_res.put(machineId, lst);
		}
		System.out.println("Partition_res:" + partition_res.toString());
		return partition_res;
	}

	/**
	 * 
	 */
	public String chooseSpareMachine() {
		Iterator<Entry<String, Integer>> iter = this.availableSlots.entrySet()
				.iterator();
		String hostId = "";
		while (iter.hasNext()) {
			Entry<String, Integer> entry = (Entry<String, Integer>) iter.next();
			String machineID = (String) entry.getKey();
			if (!this.registeredTaskTrackers.containsKey(machineID))
				continue;
			Integer res = (Integer) entry.getValue();
			if (res > 0)
				return machineID;
			hostId = machineID;
		}
		return hostId;
	}

	/**
	 * Shuffle partitions among machines, thus to ensure same key goes to same
	 * reducer
	 * 
	 * @param jobID
	 *            job's id
	 * @param mcID_hashIDs
	 */
	public void shuffle(String jobId,
			HashMap<String, List<String>> hostID_hashIDs) throws RemoteException {
		System.out.println("Shuffling ....");
		Iterator<Entry<String, List<String>>> iter = hostID_hashIDs.entrySet()
				.iterator();
		Set<String> hostIDsets = getMapperHostIDs(jobId);
		while (iter.hasNext()) {
			Iterator<String> mc_iter = hostIDsets.iterator();
			Entry<String, List<String>> entry = (Entry<String, List<String>>) iter
					.next();
			String hostID = (String) entry.getKey();
			List<String> hashIDs = (List<String>) entry.getValue();

			TaskTracker w_taskTracker = this.registeredTaskTrackers.get(hostID);
			String w_path = "/tmp/" + jobId + '/' + hostID + '/';
			while (mc_iter.hasNext()) {
				String curr = mc_iter.next().toString();
				if (curr.equals(hostID))
					continue;
				TaskTracker r_taskTracker = this.registeredTaskTrackers
						.get(curr);
				String r_path = "/tmp/" + jobId + '/' + curr + '/';
				for (int i = 0; i < hashIDs.size(); i++) {
					List<String> names = r_taskTracker.readDir(r_path + '/',
							hashIDs.get(i));
					for (int j = 0; j < names.size(); j++) {
						String content = r_taskTracker.readStr(r_path + '/',
								names.get(j));
						w_taskTracker.writeStr(w_path + '/' + names.get(j),
								content);
						System.out.println("Wrote to path:" + w_path + '/'
								+ names.get(j));
					}
				}
			}

		}
	}

	/**
	 * Get the machine IDs of the given job's mappers
	 * 
	 * @param jobID
	 *            job's id
	 * @return a set of machine IDs
	 */
	public Set<String> getMapperHostIDs(String jobID) {
		Hashtable<String, String> mapHost = jobMapperHost.get(jobID);
		Iterator<Entry<String, String>> iter = mapHost.entrySet().iterator();
		Set<String> hostIDs = new HashSet<String>();
		while (iter.hasNext()) {
			Entry<String, String> entry = (Entry<String, String>) iter.next();
			String val = (String) entry.getValue();
			hostIDs.add(val);
		}
		return hostIDs;
	}

	/**
	 * start reducer task
	 * 
	 * @param job_id
	 *            job's id
	 * @param write_path
	 *            path that reducer will write to after reducer phase is over
	 * @param mcID_hashIDs
	 */
	@Override
	public void startReducer(String jobId, String writePath,
			HashMap<String, List<String>> hostID_hashIDs) throws RemoteException {
		Iterator<Entry<String, List<String>>> iter = hostID_hashIDs.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, List<String>> entry = (Entry<String, List<String>>) iter
					.next();
			String mcID = (String) entry.getKey();
			List<String> hashIDs = (List<String>) entry.getValue();
			TaskTracker tt = this.registeredTaskTrackers.get(mcID);
			for (String id : hashIDs) {
				String reducer_id = jobId + "_r_" + id;
				Job job = this.jobId_Job.get(jobId);
				Class<? extends Reducer> reducer = job.getReducer();
				String cls_path = job.getReducerPath();
				tt.startReducer(jobId, reducer_id, writePath, reducer, cls_path);
				Hashtable<String, String> rcmc = new Hashtable<String, String>();
				rcmc.put(reducer_id, mcID);
				jobReducerHost.put(jobId, rcmc);
				job.addReduceNum();
				job.setReducer_status(reducer_id, TASK_STATUS.RUNNING);
				System.out.println("Starting Reducer in JobTracker, job_id:"
						+ jobId + ", reducer id:" + reducer_id);
			}
		}
	}

	/**
	 * Determine if task has finished
	 * 
	 * @param jobID
	 *            ID of the job
	 * @param tp
	 *            type of task (mapper/reducer)
	 * @return
	 */
	public boolean isTaskFinished(String jobID, TASK_TYPE tp) {
		System.out.println("Checking ------------------------");
		Job job = jobId_Job.get(jobID);
		HashMap<String, TASK_STATUS> task_status = null;
		if (tp == TASK_TYPE.Mapper)
			task_status = job.getMapper_status();
		else if (tp == TASK_TYPE.Reducer)
			task_status = job.getReducer_status();
		Iterator<Entry<String, TASK_STATUS>> iter = task_status.entrySet()
				.iterator();
		boolean finished = true;
		while (iter.hasNext()) {
			Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) iter
					.next();
			String mapperID = (String) pairs.getKey();
			TASK_STATUS status = (TASK_STATUS) pairs.getValue();
			if (status == TASK_STATUS.RUNNING) {
				System.out.println("Task " + mapperID + " is running");
				finished = false;
			} else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task " + mapperID + " is finished");
			else if (status == TASK_STATUS.TERMINATED)
				System.out.println("Task " + mapperID + " is terminated");
		}
		return finished;
	}

	/**
	 * restart jobs when encountered failure
	 * 
	 * @param machineID
	 *            the ID of machine that was down
	 * @throws RemoteException 
	 */
	private void restartJobs(String machineID) throws RemoteException {
		System.out.println("--------In restart jobs");
		Set<String> running_jobIDs = runningJobs.get(machineID);
		System.out.println("Running jobIDs on broken machine " + machineID
				+ ":" + running_jobIDs.toString());
		for (String jobID : running_jobIDs) {
			Job job = this.jobId_Job.get(jobID);
			HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size
					.get(jobID);
			if (mc_hash_size != null)
				mc_hash_size.remove(machineID);
			System.out.println("Terminating Job:" + jobID
					+ " because of machine:" + machineID + " is down");
			terminateJob(jobID);
			System.out.println("Restarting Job:" + jobID
					+ " because of machine:" + machineID + " is down");
			schedule(job);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.JobTracker#describeJob(mr.Job)
	 */
	@Override
	public String describeJob(String jobId) throws RemoteException {

		if (terminated)
			throw new RemoteException("JobTracker terminating");
		/* retrieve necessary data */
		Job job = jobId_Job.get(jobId);
		int nMapper = job.getMapNum();
		int nReducer = job.getReduceNum();
		/*
		 * Map<String, TASK_STATUS> mapperStatus = job.get_mapperStatus();
		 * Map<String, TASK_STATUS> reducerStatus = job.get_reducerStatus();
		 */
		/* construct status msg */
		StringBuilder sb = new StringBuilder();
		sb.append("Job ID: " + jobId + "\n");
		sb.append("Input File: " + job.getFileName() + "\n");
		sb.append("Output Path: " + job.getOutputFilePath() + "\n");
		sb.append("Mapper: " + job.getMapper().getName() + "\n");
		sb.append("#Allocated Mapper Instance: " + nMapper + "\n");
		sb.append("Reducer: " + job.getReducer().getName() + "\n");
		sb.append("#Allocated Reducer Instance: " + nReducer + "\n");
		sb.append("Job status:" + job.getJobStatus().toString());
		/*
		 * int nFinishedMapper = 0; for (Map.Entry<String, TASK_STATUS> entry :
		 * mapperStatus.entrySet()) if (entry.getValue() ==
		 * TASK_STATUS.FINISHED) nFinishedMapper++; float mapperProgress = 0; if
		 * (nMapper != 0) mapperProgress = nFinishedMapper/nMapper*100; int
		 * nFinishedReducer = 0; for (Map.Entry<String, TASK_STATUS> entry :
		 * reducerStatus.entrySet()) if (entry.getValue() ==
		 * TASK_STATUS.FINISHED) nFinishedReducer++; float reducerProgress = 0;
		 * if (nReducer != 0) reducerProgress = nFinishedReducer/nReducer*100;
		 * sb.append("Progress: Mapper " + (int)mapperProgress + "%\tReducer: "
		 * + (int)reducerProgress + "%\n");
		 */
		return sb.toString();
	}

	/**
	 * describe all jobs' status
	 */
	@Override
	public String describeJobs() throws RemoteException {
		if (terminated)
			throw new RemoteException("JobTracker terminating");
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Job> entry : jobId_Job.entrySet()) {
			sb.append("--------------------------------------------------\n");
			sb.append(describeJob(entry.getKey()));
		}
		sb.append("--------------------------------------------------\n");
		return sb.toString();
	}

	/*
	 * TaskTracker registers itself to JobTracker
	 */
	@Override
	public void register(String hostId, TaskTracker taskTracker) {
		this.registeredTaskTrackers.put(hostId, taskTracker);
	}

	/**
	 * terminate all jobs
	 */
	private void terminate_allJobs() {
		Set<Entry<String, Job>> jobset = jobId_Job.entrySet();
		Iterator<Entry<String, Job>> iter = jobset.iterator();
		while (iter.hasNext()) {
			Entry<String, Job> entry = (Entry<String, Job>) iter.next();
			String jobID = (String) entry.getKey();
			Job job = (Job) entry.getValue();
			if (job.getJobStatus() == JOB_STATUS.RUNNING) {
				System.out.println("Job " + job.getJobId()
						+ " is running, terminating it");
				terminateJob(jobID);
			}
		}
	}

	/**
	 * kill all jobs
	 */
	public void kill() throws RemoteException {
		if (this.terminated)
			return;
		this.terminate_allJobs();
	}

	/**
	 * kill a specific job based on jobID
	 * 
	 * @param jobID
	 */
	public void kill(String jobID) throws RemoteException {
		if (this.terminated)
			return;
		this.terminateJob(jobID);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
	}

	/**
	 * periodically check if TaskTrackers are still alive by sending heartbeat
	 * request to TaskTrackers
	 * @throws RemoteException 
	 */
	@Override
	public void healthCheck() throws RemoteException {
		while (true) {
			if (this.terminated)
				break;
			Set<Entry<String, TaskTracker>> set = registeredTaskTrackers
					.entrySet();
			Iterator<Entry<String, TaskTracker>> iter = set.iterator();
			while (iter.hasNext()) {
				Entry<String, TaskTracker> ent = (Entry<String, TaskTracker>) iter
						.next();
				String machineID = (String) ent.getKey();
				TaskTracker tt = (TaskTracker) ent.getValue();
				try {
					tt.heartBeat();
				} catch (RemoteException e) {
					System.out.println("Warning: TaskTracker on " + machineID
							+ " has dead");
					this.registeredTaskTrackers.remove(machineID);
					restartJobs(machineID);
				}
			}
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.JobTracker#allocateMapper(java.lang.String, java.lang.String,
	 * java.lang.String, java.lang.String, mr.Job,
	 * com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable)
	 */
	@Override
	public void allocateMapper(String hostId, String mapId, String blockId,
			String readFromHost, Job job, Hashtable<String, String> hostMapper)
			throws RemoteException {
		TaskTracker taskTracker;
		taskTracker = this.registeredTaskTrackers.get(hostId);
		taskTracker.setReducerNum(reducerNum);
		hostMapper.put(hostId, mapId);

		jobMapperHost.put(job.getJobId(), hostMapper);
		System.out.println("prepare to start mapper");

		taskTracker.startMapper(job.getJobId(), mapId, blockId, readFromHost,
				job.getMapper(), job.getMapperPath());
		System.out.println("Mapper");
		job.setMapper_status(mapId, TASK_STATUS.RUNNING);
		job.addMapNum();
		jobId_Job.put(job.getJobId(), job);
		System.out.println("Job Tracker trying to start map task on "
				+ String.valueOf(hostId) + ", mapperID:" + mapId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.JobTracker#checkHeartbeat()
	 */
	@Override
	public void checkHeartbeat(Message message) throws RemoteException {
		if (message.getTaskType() == TASK_TYPE.Mapper) {
			if (message.getTaskStat() == TASK_STATUS.FINISHED) {
				String jobID = message.getJobId();
				String taskID = message.getTaskId();
				String machineID = message.getHostId();
				System.out.println("Mapper Finished!");
				Job job = jobId_Job.get(jobID);
				job.setMapper_status(taskID, TASK_STATUS.FINISHED);
				int aval_cpus = this.availableSlots.get(machineID);
				aval_cpus++;
				availableSlots.put(machineID, aval_cpus);

				@SuppressWarnings("unchecked")
				HashMap<String, Integer> ret = (HashMap<String, Integer>) message
						.getContent();
				HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size
						.get(jobID);
				if (mc_hash_size == null) {
					HashMap<String, HashMap<String, Integer>> f_mc_hash_size = new HashMap<String, HashMap<String, Integer>>();
					f_mc_hash_size.put(machineID, ret);
					job_mc_hash_size.put(jobID, f_mc_hash_size);
				} else {
					mc_hash_size.put(machineID, ret);
				}
				if (isTaskFinished(jobID, TASK_TYPE.Mapper)) {
					System.out.println("Mapper Job finished");
					HashMap<String, List<String>> mcID_hashIDs = chooseReducer(jobID);
					System.out.println("Before Shuffle, mcID_hashIDs:"
							+ mcID_hashIDs.toString());
					shuffle(jobID, mcID_hashIDs);
					System.out.println("After Shuffle, mcID_hashIDs:"
							+ mcID_hashIDs.toString());
					startReducer(jobID, this.jobId_OutputFileDir.get(jobID),
							mcID_hashIDs);
				}
			}
		} else if (message.getTaskType() == TASK_TYPE.Reducer) {
			if (message.getTaskStat() == TASK_STATUS.FINISHED) {
				String jobID = message.getJobId();
				String taskID = message.getTaskId();
				String machineID = message.getHostId();
				Job job = jobId_Job.get(jobID);
				int aval_cpus = this.availableSlots.get(machineID);
				aval_cpus++;
				availableSlots.put(machineID, aval_cpus);
				job.setReducer_status(taskID, TASK_STATUS.FINISHED);
				if (isTaskFinished(jobID, TASK_TYPE.Reducer)) {
					System.out.println("All Reducer finished");
					job.setJobStatus(JOB_STATUS.FINISHED);
					removeFromRunningJobs(jobID);
				}
			}
		}
		if (message.getMessageType() == MESSAGE_TYPE.HEARTBEAT) {
			availableSlots.put(message.getHostId(), message.getAvalProcs());
		}

	}

	/**
	 * maintain integrity of running_jobs
	 * 
	 * @param jobID
	 */
	private void removeFromRunningJobs(String jobID) {
		Set<Entry<String, Set<String>>> set = runningJobs.entrySet();
		Iterator<Entry<String, Set<String>>> iter = set.iterator();
		while (iter.hasNext()) {
			Entry<String, Set<String>> entry = (Entry<String, Set<String>>) iter
					.next();
			Set<String> jobIDs = (Set<String>) entry.getValue();
			for (String running_jobID : jobIDs) {
				if (running_jobID.equals(jobID)) {
					jobIDs.remove(running_jobID);
				}
			}
		}
	}

	/**
	 * terminate specific job based on jobID
	 * 
	 * @param jobID
	 */
	public void terminateJob(String jobID) {
		System.out.println("--------In terminate Job");
		Job job = jobId_Job.get(jobID);
		HashMap<String, TASK_STATUS> mapper_status = null;
		HashMap<String, TASK_STATUS> reducer_status = null;
		mapper_status = job.getMapper_status();
		reducer_status = job.getReducer_status();
		terminate_mappers(jobID, mapper_status);
		terminate_reducers(jobID, reducer_status);
		job.setJobStatus(JOB_STATUS.TERMINATED);
	}

	/**
	 * terminate mapper tasks
	 * 
	 * @param jobID
	 *            ID of job
	 * @param mapper_status
	 *            status of mapper tasks of this job
	 */
	private void terminate_mappers(String jobID,
			HashMap<String, TASK_STATUS> mapper_status) {
		System.out.println("----------In terminate_mappers");
		Iterator<Entry<String, TASK_STATUS>> miter = mapper_status.entrySet()
				.iterator();
		while (miter.hasNext()) {
			Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) miter
					.next();
			String mapperID = (String) pairs.getKey();
			String machineID = jobMapperHost.get(jobID).get(mapperID);
			TASK_STATUS status = (TASK_STATUS) pairs.getValue();
			if (status == TASK_STATUS.RUNNING) {
				TaskTracker tt;
				try {
					tt = this.registeredTaskTrackers.get(machineID);
					if (tt != null) {
						tt.terminate(mapperID);
						System.out.println("Task " + mapperID
								+ " is terminated");
						this.jobId_Job.get(jobID).setMapper_status(mapperID,
								TASK_STATUS.TERMINATED);
					}
				} catch (AccessException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task " + mapperID + " is finished");
		}
	}

	/**
	 * terminate reducer tasks
	 * 
	 * @param jobID
	 *            ID of job
	 * @param reducer_status
	 *            status of reducer tasks of this job
	 */
	private void terminate_reducers(String jobID,
			HashMap<String, TASK_STATUS> reducer_status) {
		System.out.println("----------In terminate_reducers");
		Iterator<Entry<String, TASK_STATUS>> riter = reducer_status.entrySet()
				.iterator();
		while (riter.hasNext()) {
			Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) riter
					.next();
			String reducerID = (String) pairs.getKey();
			String machineID = jobReducerHost.get(jobID).get(reducerID);
			TASK_STATUS status = (TASK_STATUS) pairs.getValue();
			if (status == TASK_STATUS.RUNNING) {
				TaskTracker tt;
				try {
					tt = this.registeredTaskTrackers.get(machineID);
					if (tt != null) {
						tt.terminate(reducerID);
						System.out.println("Task " + reducerID
								+ " is terminated");
						this.jobId_Job.get(jobID).setReducer_status(reducerID,
								TASK_STATUS.TERMINATED);
					}
				} catch (AccessException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task " + reducerID + " is finished");
		}
	}

	/**
	 * terminate map reduce framework service
	 */
	@Override
	public void terminate() throws RemoteException {
		terminated = true;
		terminate_allJobs();
		System.out.println("Teminated all running jobs");
		Set<Entry<String, TaskTracker>> s = registeredTaskTrackers.entrySet();
		Iterator<Entry<String, TaskTracker>> iter = s.iterator();
		while (iter.hasNext()) {
			Entry<String, TaskTracker> entry = (Entry<String, TaskTracker>) iter
					.next();
			TaskTracker tt = (TaskTracker) entry.getValue();
			System.out.println("Terminating TaskTracker ");
			tt.terminateSelf();
		}
		try {
			mapReduceRegistry.unbind("JobTracker");
			UnicastRemoteObject.unexportObject(this, true);
			UnicastRemoteObject.unexportObject(mapReduceRegistry, true);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		JobTrackerImpl jt = new JobTrackerImpl();
		int mrPort = Integer.valueOf(args[0]);
		String dfsHost = args[1];
		int dfsPort = Integer.valueOf(args[2]);
		
		int selfPort = Integer.valueOf(args[3]);
		int reducer_ct = Integer.valueOf(args[4]);
		try {
			jt.initialize(mrPort, dfsHost, dfsPort, selfPort, reducer_ct);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public JOB_STATUS getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(JOB_STATUS jobStatus) {
		this.jobStatus = jobStatus;
	}

	public boolean isTerminated() {
		return terminated;
	}

	public void setTerminated(boolean terminated) {
		this.terminated = terminated;
	}
}
