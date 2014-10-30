package dfs;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataNodeImpl implements DataNode {
	private int id;
	private String dir;
	private String registryHost;
	private int registryPort;
	private List<String> files;

	public DataNodeImpl(int id, String dir, String registryHost,
			int registryPort) {
		this.id = id;
		this.dir = dir;
		this.registryHost = registryHost;
		this.registryPort = registryPort;
		this.files = new ArrayList<String>();
	}

	public void start() {
		try {
			Registry registry = LocateRegistry.getRegistry(registryHost,
					registryPort);
			NameNode namenode = (NameNode) registry.lookup("namenode");
			namenode.registerDataNode(this);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

	}

	public void recoverFromLocal() {
		// reconstruct the local file copies.
		File dir = new File(this.dir);
		if (dir.isDirectory()) {
			if (!dir.exists()) {
				dir.mkdirs();
				return;
			}
			String[] files = dir.list();
			Arrays.sort(files);
			String previous = "";
			for (String s : files) {
				String[] name = s.split("_");
				if (name[0].equals(previous)) {
					continue;
				} else {
					previous = name[0];
					this.files.add(previous);
				}
			}
		} else {
			System.out.println("Please input a valid directory path!");
		}
	}

	public void uploadFile(String path, String name) {

	}

	@Override
	public void createFileBlock(String filePath, String alias, BlockInfo block) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String fetchBlock(String fileName, int blockID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void terminate() {
		// TODO Auto-generated method stub
		
	}

	

}
