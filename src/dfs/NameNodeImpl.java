package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class NameNodeImpl implements NameNode {
	private Registry registry;

	private static String NAMENODE = "namenode";

	private int id = 0;
	private String dfsPath = "/tmp/dfsImage";
	private int replication = 1;
	private int interval;

	private int blockSize;
	private boolean isTerminating = false;
	private Map<String, FileInfo> fileInfos; // map from file name to
												// blockIds.
	private Integer blockId = -1; // get next blockId
	private PriorityQueue<DataNodeMeta> dataNodeHeap; //
	private HashMap<Integer, DataNodeMeta> dataNodeMap;//

	private HashMap<Integer, BlockInfo> blockInfos;
	private HashMap<Integer, DataNode> dataNodes;

	public NameNodeImpl(int registryPort, String dfs, int replication,
			int blockSize, int port, int checkInterval) {
		this.dfsPath = dfs;
		this.replication = replication;
		this.blockSize = blockSize;
		this.fileInfos = new HashMap<String, FileInfo>();
		this.dataNodeMap = new HashMap<Integer, DataNodeMeta>();
		this.dataNodeHeap = new PriorityQueue<DataNodeMeta>();
		this.dataNodes = new HashMap<Integer, DataNode>();
		this.blockInfos = new HashMap<Integer, BlockInfo>();
		this.interval = checkInterval;
		this.isTerminating = false;

		try {
			this.registry = LocateRegistry.createRegistry(registryPort);
			NameNode stub = (NameNode) UnicastRemoteObject.exportObject(this,
					port);
			this.registry.rebind(NAMENODE, stub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		this.bootstrap();
	}

	@Override
	public int getNextBlockId() {
		synchronized (blockId) {
			this.blockId++;
		}
		return this.blockId;
	}

	@Override
	public int register(DataNode node) throws RemoteException {
		synchronized (this) {
			id++;
			this.dataNodes.put(id, node);
			DataNodeMeta meta = new DataNodeMeta(id);
			meta.setState(true);
			this.dataNodeHeap.add(meta);
			this.dataNodeMap.put(id, meta);
			System.out.println("Register DataNode #" + id);
			return id;
		}
	}

	@Override
	public DataNode fetchDataNode(int id) throws RemoteException {
		return this.dataNodes.get(id);
	}

	@Override
	public FileInfo createFile(String filename, int replicas)
			throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");

		if (fileInfos.containsKey(filename)) {
			System.out.println("File Exists! Will do nothing!");
			return null;
		}

		int actualRep = replicas;
		if (replicas <= 0 || replicas > this.replication) {
			actualRep = this.replication;
		}
		FileInfo fileInfo = new FileInfo(filename, actualRep);
		return fileInfo;
	}

	@Override
	public int getBlockSize() throws RemoteException {
		return this.blockSize;
	}

	@Override
	public void commitBlockAllocation(int dataNodeId, int blockId)
			throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");
		synchronized (this) {
			if (!this.blockInfos.containsKey(blockId)) {
				this.blockInfos.put(blockId, new BlockInfo(blockId));
			}
			this.blockInfos.get(blockId).getDataNodeIds().add(dataNodeId);
			this.dataNodeMap.get(dataNodeId).getBlockIds().add(blockId);
		}
	}

	@Override
	public Map<Integer, List<Integer>> getAllBlocks(String filename)
			throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");

		FileInfo fileInfo = this.fileInfos.get(filename);

		TreeMap<Integer, List<Integer>> allBlocks = new TreeMap<Integer, List<Integer>>();
		for (Integer blockId : fileInfo.getBlockIds()) {
			allBlocks.put(blockId, this.blockInfos.get(blockId)
					.getDataNodeIds());
		}
		return allBlocks;
	}

	@Override
	public String dfsStatus() throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");

		StringBuilder dfs = new StringBuilder();
		dfs.append("========= DFS Info =========\n");
		dfs.append("#default replicas: " + this.replication + "\n");
		dfs.append("healthcheck interval: " + this.interval + " second(s)\n");
		dfs.append("block size: " + blockSize + " byte\n");
		dfs.append("========= FILE INFO =========\n");
		dfs.append("FileName\tReplicas\tBlock Ids\n");
		for (Map.Entry<String, FileInfo> entry : fileInfos.entrySet())
			dfs.append(entry.getValue().toString() + "\n");
		dfs.append("========= BLOCK INFO =========\n");
		dfs.append("BlockId\tData Node Ids\n");
		for (Map.Entry<Integer, BlockInfo> entry : blockInfos.entrySet())
			dfs.append(entry.getValue().toString() + "\n");
		dfs.append("========= DATANODE INFO =========\n");
		dfs.append("DataNode Id\tBlock Ids\tState\n");
		for (Map.Entry<Integer, DataNodeMeta> entry : dataNodeMap.entrySet())
			dfs.append(entry.getValue().toString() + "\n");
		return dfs.toString();
	}

	@Override
	public int allocateDataNode(int blockId) throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");

		DataNodeMeta meta = null;
		synchronized (this.dataNodeHeap) {
			List<DataNodeMeta> metas = new ArrayList<>();
			meta = this.dataNodeHeap.remove();
			while ((meta.getBlockIds().contains(blockId) || !meta.isLive())
					&& !this.dataNodeHeap.isEmpty()) {
				metas.add(meta);
				meta = this.dataNodeHeap.remove();
				try {
					this.fetchDataNode(meta.getId()).heartBeat();
				} catch (Exception e) {
					meta.setState(false);
					continue;
				}
			}
			if (meta.getBlockIds().contains(blockId)) {
				// run out of the available nodes.
				// every node is occupied with data now
				return -1;
			}

			for (DataNodeMeta mymeta : metas) {
				this.dataNodeHeap.add(mymeta);
			}
			this.dataNodeHeap.add(meta);
			return meta.getId();
		}

	}

	@Override
	public void updateFileInfos(String fileName, FileInfo fileInfo) {
		this.fileInfos.put(fileName, fileInfo);
	}

	@Override
	public void terminate() {
		this.isTerminating = true;
		File dfsImage = new File(this.dfsPath);
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(dfsImage));
			// begin write something important.
			oos.writeObject(this.fileInfos);
			oos.writeObject(this.blockInfos);
			oos.writeObject(this.dataNodeMap);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		/** Stop nodes */
		for (Map.Entry<Integer, DataNodeMeta> entry : dataNodeMap.entrySet()) {
			DataNodeMeta meta = entry.getValue();
			if (meta.isLive())
				try {
					this.fetchDataNode(meta.getId()).terminate();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
		}

		/**
		 * Unbind Registry
		 */
		try {
			this.registry.unbind(NAMENODE);
			UnicastRemoteObject.unexportObject(this, true);
			UnicastRemoteObject.unexportObject(registry, true);
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private void bootstrap() {
		File dfsImage = new File(this.dfsPath);
		if (!dfsImage.exists()) {
			return;
		}
		try {
			FileInputStream fis = new FileInputStream(dfsImage);
			ObjectInputStream ois = new ObjectInputStream(fis);
			this.fileInfos = (Map<String, FileInfo>) ois.readObject();
			this.blockInfos = (HashMap<Integer, BlockInfo>) ois.readObject();
			this.dataNodeMap = (HashMap<Integer, DataNodeMeta>) ois
					.readObject();
			for (Entry<Integer, DataNodeMeta> entry : this.dataNodeMap
					.entrySet()) {
				this.dataNodeHeap.add(entry.getValue());
			}

			ois.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void healthCheck() throws RemoteException {
		while (!this.isTerminating) {
			try {
				Thread.sleep(this.interval * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}// interval is based on seconds.
			for (Integer id : this.dataNodeMap.keySet()) {
				DataNodeMeta meta = this.dataNodeMap.get(id);
				if (!meta.isLive()) {
					continue;
				}

				DataNode node = this.fetchDataNode(id);
				try {
					node.heartBeat();
				} catch (RemoteException e) {
					// lose this node
					System.out.println("We lose Node " + id);
					meta.setState(false);
					List<Integer> blockIds = meta.getBlockIds();
					for (int blockId : blockIds) {
						/* get all data nodes that has this block */
						List<Integer> dataNodeIds = blockInfos.get(blockId)
								.getDataNodeIds();

						/*
						 * We cannot do anything if they all fail at the same
						 * time.
						 */
						DataNode datanode = null;
						int datanodeId = this.allocateDataNode(blockId);
						datanode = this.fetchDataNode(datanodeId);
						boolean success = false;

						for (int dataNodeId : dataNodeIds) {
							if (this.dataNodeMap.get(dataNodeId).isLive()) {
								datanode.createBlock(blockId,
										this.fetchDataNode(dataNodeId));
								success = true;
								break;
							}
						}

						if (success) {
							commitBlockAllocation(datanode.getId(), blockId);
							break;
						}
					}
				}
			}
		}
	}

	public static void main(String args[]) {

		int registryPort = 0;
		String dfs = null;
		int replication = 0;
		int blockSize = 0;
		int port = 0;
		int checkInterval = 0;
		try {
			registryPort = Integer.parseInt(args[0]);
			dfs = args[1];
			replication = Integer.parseInt(args[2]);
			blockSize = Integer.parseInt(args[3]);
			port = Integer.parseInt(args[4]);
			checkInterval = Integer.parseInt(args[5]);
		} catch (NumberFormatException e1) {
			System.out
					.println("Usage: <registry port> <fs image> <replications> <block size> <self port> <check interval>");
			System.exit(1);
		}

		NameNode node = new NameNodeImpl(registryPort, dfs, replication,
				blockSize, port, checkInterval);
		try {
			node.healthCheck();
		} catch (RemoteException e) {
			System.out.println("Server encounters severe exception!");
			System.exit(1);
		}
	}
}
