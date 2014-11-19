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

	private Registry mapReduceRegistry = null;
	private int mapReducePort = 0;

	private int jobTrackerPort = 0;
	private JOB_STATUS jobStatus = null;
	private int reducerNum = 0;

	private boolean terminated = false;

	private ExecutorService executorService = null;

	// Information of active TaskTracker with hostId
	private Hashtable<String, TaskTracker> registeredTaskTrackers = new Hashtable<String, TaskTracker>();

	// Information of hostId and available Slots
	private Hashtable<String, Integer> availableSlots = new Hashtable<String, Integer>();

	private Hashtable<String, Set<String>> runningJobs = new Hashtable<String, Set<String>>();

	// Information of Job and related MapId_hostId
	private Hashtable<String, Hashtable<String, String>> jobMapperHost = new Hashtable<String, Hashtable<String, String>>();
	private Hashtable<String, Hashtable<String, String>> jobReducerHost = new Hashtable<String, Hashtable<String, String>>();

	private Hashtable<String, Job> jobMap = new Hashtable<String, Job>();

	// Information of JobId, MachineId, partitionId, size
	HashMap<String, HashMap<String, HashMap<String, Integer>>> job_host_hash_size = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
	// jobId //hostId //partitionId size
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
			jobMap.put(job.getJobId(), job);
			Set<?> set = mappings.entrySet();
			/*
			 * if (job.getOutputFilePath() != null)
			 * jobIdOutputDir.put(job.getJobId(), job.getOutputFilePath());
			 */
			System.out.println("Scheduling Job " + job.getJobId());
			// Hashtable<String, String> host_mp = new Hashtable<String,
			// String>();
			for (Iterator<?> iter = set.iterator(); iter.hasNext();) {
				@SuppressWarnings("rawtypes")
				Entry entry = (Entry) iter.next();
				Integer blockId = (Integer) entry.getKey();
				@SuppressWarnings("unchecked")
				List<Integer> value = (List<Integer>) entry.getValue();
				System.out.println("BlockId:" + blockId.toString());
				/* set mapper task */
				String mapId = job.getJobId() + "_m_" + String.valueOf(blockId);
				boolean allocated = false;
				for (Integer hostId : value) {
					if (!this.registeredTaskTrackers.containsKey(String
							.valueOf(hostId)))
						continue;
					System.out.println("MachineId:" + hostId.toString());
					int availableSlots = this.availableSlots.get(String
							.valueOf(hostId));
					System.out.println("Aval Slots NUM:" + availableSlots);
					if (availableSlots > 0) {
						System.out.println("Availble CPU, machine: " + hostId);
						String readFromHost = String.valueOf(hostId);
						allocateMapper(String.valueOf(hostId), mapId,
								String.valueOf(blockId), readFromHost, job);
						availableSlots--;
						this.availableSlots.put(String.valueOf(hostId),
								availableSlots);
						allocated = true;
						Set<String> runningJobIds = runningJobs.get(hostId);
						if (runningJobIds == null) {
							runningJobIds = new HashSet<String>();
							runningJobIds.add(job.getJobId());
							runningJobs.put(String.valueOf(hostId),
									runningJobIds);
						} else {
							runningJobIds.add(job.getJobId());
						}
						break;
					}
				}
				if (!allocated) {
					String hostId = chooseSpareMachine();
					if (!hostId.equals("")) {
						int avaiSlots = this.availableSlots.get(hostId);
						String readFromMachine = String.valueOf(value.get(0));
						allocateMapper(hostId, mapId, String.valueOf(blockId),
								readFromMachine, job); // , host_mp);
						avaiSlots--;
						this.availableSlots.put(hostId, avaiSlots);
						allocated = true;
						Set<String> runningJobIds = runningJobs.get(hostId);
						if (runningJobIds == null) {
							runningJobIds = new HashSet<String>();
							runningJobIds.add(job.getJobId());
							runningJobs.put(hostId, runningJobIds);
						} else {
							runningJobIds.add(job.getJobId());
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
	 * @param jobId
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

	/**
	 * Update: choose it randomly.
	 */
	@Override
	public HashMap<String, List<String>> chooseReducer(String jobId)
			throws RemoteException {
		/* partition_res: mapping of machineId and hashedId */
		HashMap<String, List<String>> partitionResult = new HashMap<String, List<String>>();
		HashMap<String, HashMap<String, Integer>> host_hash_size = job_host_hash_size
				.get(jobId);
		try {
			System.out.println("Choosing Partition! size: "
					+ host_hash_size.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		/* allocate #reducer_ct reducers */
		for (int i = 0; i < reducerNum; i++) {
			System.out.println("*****************Reducer #" + i
					+ "  *****************");
			TreeMap<Integer, String> priorityQ = new TreeMap<Integer, String>();
			/* insert each record into a priority queue */
			for (Entry<String, HashMap<String, Integer>> entry : host_hash_size
					.entrySet()) {
				System.out.println("********* Entry: " + entry.toString());
				Integer size = entry.getValue().get(String.valueOf(i));
				if (size == null) {
					size = 0;
				}
				priorityQ.put(size, entry.getKey());
				System.out.println("********* PriorityQ:"
						+ priorityQ.toString());
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
			List<String> lst = partitionResult.get(machineId);
			if (lst == null)
				lst = new ArrayList<String>();
			lst.add(String.valueOf(i));
			partitionResult.put(machineId, lst);
		}
		System.out.println("PartitionRes:" + partitionResult.toString());
		return partitionResult;
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
			String machineId = (String) entry.getKey();
			if (!this.registeredTaskTrackers.containsKey(machineId))
				continue;
			Integer res = (Integer) entry.getValue();
			if (res > 0)
				return machineId;
			hostId = machineId;
		}
		return hostId;
	}

	/**
	 * Shuffle partitions among machines, thus to ensure same key goes to same
	 * reducer
	 * 
	 * @param jobId
	 *            job's id
	 * @param hostId_hashIds
	 */
	public void shuffle(String jobId,
			HashMap<String, List<String>> hostId_hashIds)
			throws RemoteException {
		System.out.println("Shuffling ....");
		try {
			Iterator<Entry<String, List<String>>> iter = hostId_hashIds
					.entrySet().iterator();
			Set<String> hostIdsets = getMapperHostIds(jobId);
			while (iter.hasNext()) {
				Iterator<String> hostIter = hostIdsets.iterator();
				Entry<String, List<String>> entry = (Entry<String, List<String>>) iter
						.next();
				String hostId = (String) entry.getKey();
				List<String> hashIds = (List<String>) entry.getValue();

				TaskTracker wTaskTracker = this.registeredTaskTrackers
						.get(hostId);

				System.out.println("HashIds : " + hashIds.toString());
				System.out.println("hostIdsets : " + hostIdsets.toString());

				// System.out.println("wTaskTracker is null?"
				// + (wTaskTracker == null)); false

				String wPath = "/tmp/" + jobId + "/" + hostId + "/";
				while (hostIter.hasNext()) {
					String curr = hostIter.next().toString();
					if (curr.equals(hostId))
						continue;
					TaskTracker rTaskTracker = this.registeredTaskTrackers
							.get(curr);
					String rPath = "/tmp/" + jobId + "/" + curr + "/";
					for (int i = 0; i < hashIds.size(); i++) {
						System.out.println("Dir To Be Read: " + rPath
								+ "\t num #" + hashIds.get(i));

						List<String> names = rTaskTracker.readDir(rPath + '/',
								hashIds.get(i));
						for (int j = 0; j < names.size(); j++) {
							String content = rTaskTracker.readStr(rPath,
									names.get(j));
							wTaskTracker.writeStr(wPath + '/' + names.get(j),
									content);
							System.out.println("Wrote to path:" + wPath
									+ names.get(j));
						}
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the machine Ids of the given job's mappers
	 * 
	 * @param jobId
	 *            job's id
	 * @return a set of machine Ids
	 */
	public Set<String> getMapperHostIds(String jobId) {
		Hashtable<String, String> mapHost = jobMapperHost.get(jobId);
		Iterator<Entry<String, String>> iter = mapHost.entrySet().iterator();
		Set<String> hostIds = new HashSet<String>();
		while (iter.hasNext()) {
			Entry<String, String> entry = (Entry<String, String>) iter.next();
			String val = (String) entry.getValue();
			hostIds.add(val);
		}
		return hostIds;
	}

	/**
	 * start reducer task
	 * 
	 * @param job_id
	 *            job's id
	 * @param write_path
	 *            path that reducer will write to after reducer phase is over
	 * @param hostId_hashIds
	 */
	@Override
	public void startReducer(String jobId, String writePath,
			HashMap<String, List<String>> hostId_hashIds)
			throws RemoteException {
		Iterator<Entry<String, List<String>>> iter = hostId_hashIds.entrySet()
				.iterator();
		while (iter.hasNext()) {
			Entry<String, List<String>> entry = (Entry<String, List<String>>) iter
					.next();
			String hostId = (String) entry.getKey();
			List<String> hashIds = (List<String>) entry.getValue();
			TaskTracker tt = this.registeredTaskTrackers.get(hostId);
			for (String id : hashIds) {
				String reducerId = jobId + "_r_" + id;
				Job job = this.jobMap.get(jobId);
				Class<? extends Reducer> reducer = job.getReducer();
				String clsPath = job.getReducerPath();
				tt.startReducer(jobId, reducerId, writePath, reducer, clsPath);

				if (!jobReducerHost.containsKey(jobId)) {
					jobReducerHost.put(jobId, new Hashtable<String, String>());
				}
				jobReducerHost.get(jobId).put(reducerId, hostId);

				job.addReducerNum();
				job.setReducerStatus(reducerId, TASK_STATUS.RUNNING);
				System.out.println("Starting Reducer in JobTracker, job_id:"
						+ jobId + ", reducer id:" + reducerId);
			}
		}
	}

	/**
	 * Determine if task has finished
	 * 
	 * @param jobId
	 *            Id of the job
	 * @param tp
	 *            type of task (mapper/reducer)
	 * @return
	 */
	public boolean isTaskFinished(String jobId, TASK_TYPE tp) {
		System.out.println("Checking ------------------------");
		Job job = jobMap.get(jobId);
		HashMap<String, TASK_STATUS> taskStatus = null;
		if (tp == TASK_TYPE.Mapper)
			taskStatus = job.getMapperStatus();
		else if (tp == TASK_TYPE.Reducer)
			taskStatus = job.getReducerStatus();
		Iterator<Entry<String, TASK_STATUS>> iter = taskStatus.entrySet()
				.iterator();
		boolean finished = true;
		while (iter.hasNext()) {
			Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) iter
					.next();
			String mapperId = (String) pairs.getKey();
			TASK_STATUS status = (TASK_STATUS) pairs.getValue();
			if (status == TASK_STATUS.RUNNING) {
				System.out.println("Task " + mapperId + " is running");
				finished = false;
			} else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task " + mapperId + " is finished");
			else if (status == TASK_STATUS.TERMINATED)
				System.out.println("Task " + mapperId + " is terminated");
		}
		return finished;
	}

	/**
	 * restart jobs when encountered failure
	 * 
	 * @param machineId
	 *            the Id of machine that was down
	 * @throws RemoteException
	 */
	private void restartJobs(String machineId) throws RemoteException {
		System.out.println("--------In restart jobs");
		Set<String> running_jobIds = runningJobs.get(machineId);
		System.out.println("Running jobIds on broken machine " + machineId
				+ ":" + running_jobIds.toString());
		for (String jobId : running_jobIds) {
			Job job = this.jobMap.get(jobId);
			HashMap<String, HashMap<String, Integer>> host_hash_size = job_host_hash_size
					.get(jobId);
			if (host_hash_size != null)
				host_hash_size.remove(machineId);
			System.out.println("Terminating Job:" + jobId
					+ " because of machine:" + machineId + " is down");
			terminateJob(jobId);
			System.out.println("Restarting Job:" + jobId
					+ " because of machine:" + machineId + " is down");
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
		Job job = jobMap.get(jobId);
		int nMapper = job.getMapNum();
		int nReducer = job.getReduceNum();
		/*
		 * Map<String, TASK_STATUS> mapperStatus = job.get_mapperStatus();
		 * Map<String, TASK_STATUS> reducerStatus = job.get_reducerStatus();
		 */
		/* construct status msg */
		StringBuilder sb = new StringBuilder();
		sb.append("Job Id: " + jobId + "\n");
		sb.append("Input File: " + job.getFileName() + "\n");
		sb.append("Output Path: " + job.getOutputFilePath() + "\n");
		sb.append("Mapper: " + job.getMapper().getName() + "\n");
		sb.append("#Allocated Mapper Instance: " + nMapper + "\n");
		sb.append("Reducer: " + job.getReducer().getName() + "\n");
		sb.append("#Allocated Reducer Instance: " + nReducer + "\n");
		sb.append("Job status:" + job.getJobStatus().toString());
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
		for (Map.Entry<String, Job> entry : jobMap.entrySet()) {
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
	private void terminateAllJobs() {
		Set<Entry<String, Job>> jobset = jobMap.entrySet();
		Iterator<Entry<String, Job>> iter = jobset.iterator();
		while (iter.hasNext()) {
			Entry<String, Job> entry = (Entry<String, Job>) iter.next();
			String jobId = (String) entry.getKey();
			Job job = (Job) entry.getValue();
			if (job.getJobStatus() == JOB_STATUS.RUNNING) {
				System.out.println("Job " + job.getJobId()
						+ " is running, terminating it");
				terminateJob(jobId);
			}
		}
	}

	/**
	 * kill all jobs
	 */
	@Override
	public void kill() throws RemoteException {
		if (this.terminated)
			return;
		this.terminateAllJobs();
	}

	/**
	 * kill a specific job based on jobId
	 * 
	 * @param jobId
	 */
	@Override
	public void kill(String jobId) throws RemoteException {
		if (this.terminated)
			return;
		this.terminateJob(jobId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			this.healthCheck();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * periodically check if TaskTrackers are still alive by sending heartbeat
	 * request to TaskTrackers
	 * 
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
				String machineId = (String) ent.getKey();
				TaskTracker tt = (TaskTracker) ent.getValue();
				try {
					tt.heartBeat();
				} catch (RemoteException e) {
					System.out.println("Warning: TaskTracker on " + machineId
							+ "is dead");
					this.registeredTaskTrackers.remove(machineId);
					restartJobs(machineId);
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
			String readFromHost, Job job) throws RemoteException {
		TaskTracker taskTracker;
		taskTracker = this.registeredTaskTrackers.get(hostId);
		taskTracker.setReducerNum(reducerNum);

		if (!jobMapperHost.containsKey(job.getJobId())) {
			jobMapperHost.put(job.getJobId(), new Hashtable<String, String>());
		}

		jobMapperHost.get(job.getJobId()).put(mapId, hostId);

		System.out.println("prepare to start mapper");

		taskTracker.startMapper(job.getJobId(), mapId, blockId, readFromHost,
				job.getMapper(), job.getMapperPath());
		System.out.println("Mapper");
		job.setMapperStatus(mapId, TASK_STATUS.RUNNING);
		job.addMapNum();
		jobMap.put(job.getJobId(), job);
		System.out.println("Job Tracker trying to start map task on Host "
				+ String.valueOf(hostId) + ", mapperId:" + mapId);

		// prepare the job_host_hash_size. Since we decide to randomize it the
		// choosing, we make it 0 everywhere.
		if (this.job_host_hash_size.get(job.getJobId()) == null) {
			this.job_host_hash_size.put(job.getJobId(),
					new HashMap<String, HashMap<String, Integer>>());
		}
		if (this.job_host_hash_size.get(job.getJobId()).get(hostId) == null) {
			this.job_host_hash_size.get(job.getJobId()).put(hostId,
					new HashMap<String, Integer>());

			for (int i = 0; i < reducerNum; i++) {
				this.job_host_hash_size.get(job.getJobId()).get(hostId)
						.put(i + "", 0);
			}
		}

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
				String jobId = message.getJobId();
				String taskId = message.getTaskId();
				String machineId = message.getHostId();
				System.out.println("Mapper Finished!");
				Job job = jobMap.get(jobId);
				job.setMapperStatus(taskId, TASK_STATUS.FINISHED);
				int avalCPUs = this.availableSlots.get(machineId);
				avalCPUs++;
				availableSlots.put(machineId, avalCPUs);

				if (isTaskFinished(jobId, TASK_TYPE.Mapper)) {
					System.out.println("Mapper Job with JobId " + jobId
							+ " finished");
					HashMap<String, List<String>> hostIdHashIds = this
							.chooseReducer(jobId);
					System.out.println("Before Shuffle, hostId_hashIds:"
							+ hostIdHashIds.toString());
					shuffle(jobId, hostIdHashIds);
					System.out.println("After Shuffle, hostId_hashIds:"
							+ hostIdHashIds.toString());
					startReducer(jobId, this.jobMap.get(jobId)
							.getOutputFilePath(), hostIdHashIds);
				}
			}
		} else if (message.getTaskType() == TASK_TYPE.Reducer) {
			if (message.getTaskStat() == TASK_STATUS.FINISHED) {
				String jobId = message.getJobId();
				String taskId = message.getTaskId();
				String machineId = message.getHostId();
				Job job = jobMap.get(jobId);
				int aval_cpus = this.availableSlots.get(machineId);
				aval_cpus++;
				availableSlots.put(machineId, aval_cpus);
				job.setReducerStatus(taskId, TASK_STATUS.FINISHED);
				if (isTaskFinished(jobId, TASK_TYPE.Reducer)) {
					System.out.println("All Reducer finished");
					job.setJobStatus(JOB_STATUS.FINISHED);
					removeFromRunningJobs(jobId);
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
	 * @param jobId
	 */
	private void removeFromRunningJobs(String jobId) {
		Set<Entry<String, Set<String>>> set = runningJobs.entrySet();
		Iterator<Entry<String, Set<String>>> iter = set.iterator();
		while (iter.hasNext()) {
			Entry<String, Set<String>> entry = (Entry<String, Set<String>>) iter
					.next();
			Set<String> jobIds = (Set<String>) entry.getValue();
			for (String running_jobId : jobIds) {
				if (running_jobId.equals(jobId)) {
					jobIds.remove(running_jobId);
				}
			}
		}
	}

	/**
	 * terminate specific job based on jobId
	 * 
	 * @param jobId
	 */
	public void terminateJob(String jobId) {
		System.out.println("--------In terminate Job");
		Job job = jobMap.get(jobId);
		HashMap<String, TASK_STATUS> mapperStatus = null;
		HashMap<String, TASK_STATUS> reducerStatus = null;
		mapperStatus = job.getMapperStatus();
		reducerStatus = job.getReducerStatus();
		terminateMappers(jobId, mapperStatus);
		terminateReducers(jobId, reducerStatus);
		job.setJobStatus(JOB_STATUS.TERMINATED);
	}

	/**
	 * terminate mapper tasks
	 * 
	 * @param jobId
	 *            Id of job
	 * @param mapper_status
	 *            status of mapper tasks of this job
	 */
	private void terminateMappers(String jobId,
			HashMap<String, TASK_STATUS> mapper_status) {
		System.out.println("----------In terminate_mappers");
		Iterator<Entry<String, TASK_STATUS>> miter = mapper_status.entrySet()
				.iterator();
		while (miter.hasNext()) {
			Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) miter
					.next();
			String mapperId = (String) pairs.getKey();
			String machineId = jobMapperHost.get(jobId).get(mapperId);
			TASK_STATUS status = (TASK_STATUS) pairs.getValue();
			if (status == TASK_STATUS.RUNNING) {
				TaskTracker tt;
				try {
					tt = this.registeredTaskTrackers.get(machineId);
					if (tt != null) {
						tt.terminate(mapperId);
						System.out.println("Task " + mapperId
								+ " is terminated");
						this.jobMap.get(jobId).setMapperStatus(mapperId,
								TASK_STATUS.TERMINATED);
					}
				} catch (AccessException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task " + mapperId + " is finished");
		}
	}

	/**
	 * terminate reducer tasks
	 * 
	 * @param jobId
	 *            Id of job
	 * @param reducerStatus
	 *            status of reducer tasks of this job
	 */
	private void terminateReducers(String jobId,
			HashMap<String, TASK_STATUS> reducerStatus) {
		System.out.println("----------In terminate_reducers");
		Iterator<Entry<String, TASK_STATUS>> riter = reducerStatus.entrySet()
				.iterator();
		while (riter.hasNext()) {
			Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) riter
					.next();
			String reducerId = (String) pairs.getKey();
			String machineId = jobReducerHost.get(jobId).get(reducerId);
			TASK_STATUS status = (TASK_STATUS) pairs.getValue();
			if (status == TASK_STATUS.RUNNING) {
				TaskTracker tt;
				try {
					tt = this.registeredTaskTrackers.get(machineId);
					if (tt != null) {
						tt.terminate(reducerId);
						System.out.println("Task " + reducerId
								+ " is terminated");
						this.jobMap.get(jobId).setReducerStatus(reducerId,
								TASK_STATUS.TERMINATED);
					}
				} catch (AccessException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task " + reducerId + " is finished");
		}
	}

	/**
	 * terminate map reduce framework service
	 */
	@Override
	public void terminate() throws RemoteException {
		terminated = true;
		terminateAllJobs();
		System.out.println("Teminated all running jobs");
		Set<Entry<String, TaskTracker>> s = registeredTaskTrackers.entrySet();
		Iterator<Entry<String, TaskTracker>> iter = s.iterator();
		while (iter.hasNext()) {
			Entry<String, TaskTracker> entry = (Entry<String, TaskTracker>) iter
					.next();
			TaskTracker tt = (TaskTracker) entry.getValue();
			System.out.println("Terminating TaskTracker!");
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
		int reducerCount = Integer.valueOf(args[4]);
		try {
			jt.initialize(mrPort, dfsHost, dfsPort, selfPort, reducerCount);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

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
