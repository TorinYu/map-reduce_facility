package dfs;

import java.io.File;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class NameNodeImpl implements NameNode {
	private Registry registry;
	private static String DATA = "data";
	private int id = 0;
	private String dfsPath = "/tmp/dfs/";
	private int replication = 1;
	private HashMap<String, FileInfo> files;
	private int blockSize;
	private boolean isTerminating = false;
	private Map<String, List<Integer>> fileBlocks;

	public NameNodeImpl(int portNumber, String dfs, int replication,
			int blockSize) {
		this.dfsPath = dfs;
		this.replication = replication;
		this.blockSize = blockSize;
		try {
			this.registry = LocateRegistry.createRegistry(portNumber);
			this.registry.bind("namenode", this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (AlreadyBoundException e) {
			// e.printStackTrace();
			// actually do nothing here.
		}
	}

	@Override
	public void terminate() {
		this.isTerminating = true;
		// TODO: Add terminate
	}

	public void uploadFile(String fileName) {
		File file = new File(fileName);
		long length = file.length();
		int blockCount = (int) Math.round(length * 1.0 / this.blockSize);

	}

	public DataNode searchDataNode(String fileName, int blockId) {
		DataNode node = null;
		if (files.containsKey(fileName)) {
			FileInfo fileInfo = files.get(fileName);
			BlockInfo b = fileInfo.getBlocks().get(blockId);
			Random r = new Random();
			int index = r.nextInt(b.getDataNodeIds().size());
			int nodeNum = b.getDataNodeIds().get(index);
			try {
				node = (DataNode) this.registry.lookup(DATA + nodeNum);
			} catch (AccessException e) {
				e.printStackTrace();
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			}
		}
		return node;
	}

	@Override
	public int register(DataNode node) throws RemoteException {
		synchronized (this) {
			id++;
			try {
				registry.bind(DATA + id, node);
			} catch (AccessException e) {
				e.printStackTrace();
				return -1;
			} catch (RemoteException e) {
				e.printStackTrace();
				return -1;
			} catch (AlreadyBoundException e) {
				e.printStackTrace();
				return -1;
			}
			return id;
		}
	}

	@Override
	public DataNode fetchDataNode(int id) throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");
		DataNode node = null;
		try {
			node = (DataNode) registry.lookup(DATA + id);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		return node;
	}

	@Override
	public int createFile(String filename, int replicas) throws RemoteException {
		if (isTerminating)
			throw new RemoteException("DFS terminating");
		int actualRep = replicas;
		if (replicas > this.replication) {
			actualRep = this.replication;
		}
		
		return actualRep;
	}

	@Override
	public int getBlockSize() throws RemoteException {
		return this.blockSize;
	}

	@Override
	public DataNode allocateBlock() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commitBlockAllocation(int dataNodeId, String filename,
			int blockId) throws RemoteException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<Integer, List<Integer>> getAllBlocks(String filename)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String dfsStatus() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void uploadFile(String fileName, String alias) {
		// TODO Auto-generated method stub

	}

}
