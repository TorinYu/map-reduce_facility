package dfs;

import java.io.File;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;

public class NameNodeImpl implements NameNode {
	private Registry registry;
	private static String DATA = "data";
	private int id = 0;
	private String dfsPath = "/tmp/dfs/";
	private int replication = 1;
	private HashMap<String, FileMeta> files;

	public NameNodeImpl(int portNumber, String dfs, int replication) {
		this.dfsPath = dfs;
		this.replication = replication;

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
	public int registerDataNode(DataNode node) {
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
	public boolean uploadFile(String fileName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public File downloadFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean terminate(int datanode) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean terminate() {
		// TODO Auto-generated method stub
		return false;
	}

}
