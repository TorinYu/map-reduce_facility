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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import dfs.Downloader;
import dfs.FileUploader;
import mr.Type.MESSAGE_TYPE;
import mr.Type.TASK_STATUS;
import mr.Type.TASK_TYPE;

/**
 * @author Nicolas_Yu
 * 
 */
public class TaskTrackerImpl implements TaskTracker, Runnable {

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
	int reducerNum = 0;

	boolean terminated = false;

	HashMap<String, Future<?>> taskFuture = new HashMap<String, Future<?>>();

	ExecutorService executorService = null;

	LinkedBlockingQueue<Message> heartbeats = new LinkedBlockingQueue<Message>();

	
	/**
	 * Constructor of TaskTracker 
	 * 
	 * @param mrRegistryHost
	 * @param mrPort
	 * @param dfsHost
	 * @param dfsPort
	 * @param selfPort
	 * @param taskId
	 * @param reducerNum
	 * @param inputDir
	 */
	public TaskTrackerImpl(String mrRegistryHost, int mrPort, String dfsHost,
			int dfsPort, int selfPort, int taskId, int reducerNum,
			String inputDir) {
		try {

			this.mapReduceRegistryHost = mrRegistryHost;
			this.mapReducePort = mrPort;

			this.hdfsRegistryHost = dfsHost;
			this.hdfsPort = dfsPort;

			mapReduceRegistry = LocateRegistry.getRegistry(
					this.mapReduceRegistryHost, this.mapReducePort);
			this.inputDir = inputDir;
			jobTracker = (JobTracker) mapReduceRegistry.lookup("JobTracker");
			this.reducerNum = reducerNum;
			this.port = selfPort;

			this.hostId = String.valueOf(taskId);

			executorService = Executors.newCachedThreadPool();
			executorService.submit(this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * terminate a task
	 * 
	 * @param taskID
	 *            ID of the task
	 */
	public void terminate(String taskID) {
		Future<?> f = taskFuture.get(taskID);
		((java.util.concurrent.Future<?>) f).cancel(true);
		System.out.println("Task " + taskID + " terminated");
	}

	/**
	 * Initiliaze the taskTracker
	 */
	public void initialize() {
		try {
			TaskTracker stub = (TaskTracker) UnicastRemoteObject.exportObject(
					this, port);
			jobTracker.register(hostId, stub);
			Message initialMsg = new Message();
			Integer avalProcs = 20; // The default slots on one TaskTracker is 20
			initialMsg.setAvalProcs(avalProcs);
			initialMsg.setMessageType(MESSAGE_TYPE.HEARTBEAT);
			initialMsg.setHostId(hostId);
			this.heartbeats.offer(initialMsg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.TaskTracker#setReduceNum()
	 */
	@Override
	public void setReducerNum(int reducerNum) {
		this.reducerNum = reducerNum;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.TaskTracker#startMapper()
	 */
	@Override
	public void startMapper(String jobId, String mapId, String blockId,
			String readFromHost, Class<? extends Mapper> mapper,
			String mapperPath) {
		Downloader dn = new Downloader(hdfsRegistryHost, hdfsPort);
		dn.download(mapperPath, mapperPath);

		Task task = new Task(jobId, mapId, hdfsRegistryHost, hdfsPort);
		task.setTaskType(Type.TASK_TYPE.Mapper);
		task.setBlockId(Integer.parseInt(blockId));
		task.setReducerNum(reducerNum);
		task.setMapper(mapper);
		task.setReadDir(inputDir);
		task.setHostId(hostId);
		task.setDataNodeId(Integer.parseInt(readFromHost));
		Future<?> f1 = executorService.submit(task);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.TaskTracker#startReducer()
	 */
	@Override
	public void startReducer(String jobId, String reducerId, String writePath,
			Class<? extends Reducer> reducer, String clspath) {
		try {
			Downloader dn = new Downloader(hdfsRegistryHost, hdfsPort);
			dn.download(clspath, clspath);

			System.out.println("Download Finishes!");
			Task task = new Task(jobId, reducerId, hdfsRegistryHost, hdfsPort);
			task.setTaskType(Type.TASK_TYPE.Reducer);
			task.setReducer(reducer);
			task.setReducerNum(reducerNum);
			task.setHostId(hostId);
			Future<?> f1 = (Future<?>) executorService.submit(task);
			taskFuture.put(reducerId, f1);

			Message msg = new Message();
			msg.setJobId(jobId);
			msg.setTaskId(reducerId);
			msg.setTaskType(TASK_TYPE.Reducer);
			msg.setTaskStat(TASK_STATUS.RUNNING);
			msg.setHostId(String.valueOf(hostId));
			msg.setOutputPath(writePath);
			msg.setFuture(f1);
			this.heartbeats.offer(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Write content to file
	 */
	@Override
	public void writeFile(String path, byte[] content) {
		FileOutputStream writeFile;
		try {
			writeFile = new FileOutputStream(path, true);
			writeFile.write(content);
			writeFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void writeStr(String path, String content) {
		try {
			File file = new File(path.substring(0, path.lastIndexOf("/")));
			if (!file.exists()) {
				file.mkdirs();
			}
			BufferedWriter out = new BufferedWriter(new FileWriter(path, true));
			out.write(content + '\n');
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * read all partitioned files from dir based on the hashIds
	 * 
	 * @param path
	 *            DIR to read from
	 * @param hashID
	 *            partitions are hashed based on reducer numbers
	 */
	@Override
	public List<String> readDir(String path, String hashID) {
		File dir = new File(path);
		File[] files = dir.listFiles();
		List<String> retNames = new ArrayList<String>();
		for (int i = 0; i < files.length; i++) {
			String name = files[i].getName();
			String tmp[] = name.split("#");
			if (tmp.length > 0)

				if (tmp[tmp.length - 1].equals(hashID)) {
					retNames.add(name);
				}
		}
		return retNames;
	}

	/*
	 * Read String content from file
	 */
	@Override
	public String readStr(String path, String name) {
		StringBuilder content = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(path + '/'
					+ name));
			String line = null;
			while ((line = br.readLine()) != null)
				content.append(line + '\n');
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see mr.TaskTracker#sendHeartbeat()
	 */
	@Override
	public void heartBeat() {
	}

	/**
	 *  checks if mapper is finished or terminated, because of asynchronous
	 * 
	 * @param msg
	 */
	private void checkMapper(Message msg) {
		Future<?> f = msg.getFuture();
		if (f != null) {
			if (f.isDone()) {
				try {
					if (f.get() == TASK_STATUS.TERMINATED) {
						msg.setTaskStat(TASK_STATUS.TERMINATED);
						msg.setFuture(null);
						jobTracker.checkHeartbeat(msg);
					} else {
						msg.setFuture(null);
						msg.setContent(null);
						msg.setTaskStat(TASK_STATUS.FINISHED);
						jobTracker.checkHeartbeat(msg);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else if (f.isCancelled()) {
				msg.setTaskStat(TASK_STATUS.TERMINATED);
				msg.setFuture(null);
				try {
					jobTracker.checkHeartbeat(msg);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else {
				this.heartbeats.add(msg);
			}
		}
	}

	/**
	 * reducers are asynchronous, this function checks if reducer is finished or
	 * terminated
	 * 
	 * @param msg
	 */
	private void checkReducer(Message msg) {
		Future<?> f1 = msg.getFuture();
		String reducerId = msg.getTaskId();
		String outputPath = msg.getOutputPath();
		System.out.println("reducer is " + reducerId);
		System.out.println("output path is " + outputPath);
		System.out.println("f1 is null? " + (f1 == null));

		if (f1 != null) {
			if (f1.isDone()) {
				try {
					if (f1.get() != null) {
						if (f1.get() == TASK_STATUS.TERMINATED) {
							msg.setTaskStat(TASK_STATUS.TERMINATED);
							msg.setFuture(null);
							jobTracker.checkHeartbeat(msg);
						}
						String path = outputPath + '/' + reducerId;
						System.out.println("Path is " + path);

						FileUploader uploader = new FileUploader(
								hdfsRegistryHost, hdfsPort);
						uploader.upload("/tmp/" + reducerId + "/0", 0, path);
						System.out.println("output path:" + path);
						System.out.println("reducerID:" + reducerId);
						System.out.println("Writing to DFS, REDUCER ID:"
								+ reducerId);

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
				}
			} else if (f1.isCancelled()) {
				msg.setTaskStat(TASK_STATUS.TERMINATED);
				try {
					msg.setFuture(null);
					jobTracker.checkHeartbeat(msg);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else {
				this.heartbeats.add(msg);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while (true) {
			if (this.terminated) {
				return;
			}

			Message msg = this.heartbeats.poll();
			if (msg != null) {
				if (msg.getTaskType() == TASK_TYPE.Mapper) {
					checkMapper(msg);
				} else if (msg.getTaskType() == TASK_TYPE.Reducer) {
					checkReducer(msg);
				} else
					try {
						jobTracker.checkHeartbeat(msg);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
			} else {
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * terminate this TaskTracker
	 */
	@Override
	public void terminateSelf() throws RemoteException {
		System.out.println("TaskTracker " + this.hostId + " is terminating");
		this.terminated = true;
		UnicastRemoteObject.unexportObject(this, true);
		System.out.println("TaskTracker " + this.hostId + " terminated");
	}

	public static void main(String[] args) {
		String mrRegistryHost = args[0];
		int mrPort = Integer.valueOf(args[1]);
		String dfsHost = args[2];
		int dfsPort = Integer.valueOf(args[3]);
		int selfPort = Integer.valueOf(args[4]);
		int id = Integer.valueOf(args[5]);
		String read_dir = args[6];
		int reducer_ct = Integer.valueOf(args[7]);
		TaskTrackerImpl tt = new TaskTrackerImpl(mrRegistryHost, mrPort,
				dfsHost, dfsPort, selfPort, id, reducer_ct, read_dir);
		tt.initialize();
	}

}
