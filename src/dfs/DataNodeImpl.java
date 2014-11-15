package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class DataNodeImpl implements DataNode {
	private int id;
	private String dir;
	private String registryHost;
	private int registryPort;
	private List<Integer> blockIds;
	private static final String NAMENODE = "namenode";
	private int myPort = 0;

	public DataNodeImpl(String registryHost, int registryPort, String dir,
			int myPort) {
		this.dir = dir;
		this.registryHost = registryHost;
		this.registryPort = registryPort;
		this.blockIds = new ArrayList<Integer>();
		this.myPort = myPort;
		this.start();
	}

	public static void main(String args[]) {
		String registryHost = args[0];
		int registryPort = Integer.parseInt(args[1]);
		String dir = args[2];
		int myPort = Integer.parseInt(args[3]);
		DataNode datanode = new DataNodeImpl(registryHost, registryPort, dir,
				myPort);
	}

	private void start() {
		try {
			DataNode stub = (DataNode) UnicastRemoteObject.exportObject(this,
					myPort);
			while (true) {
				Registry registry = LocateRegistry.getRegistry(registryHost,
						registryPort);
				NameNode namenode = (NameNode) registry.lookup(NAMENODE);
				int id = namenode.register(stub);
				this.setId(id);
				break;
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		File dir = new File(this.dir);
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

	@Override
	public void terminate() {
		try {
			UnicastRemoteObject.unexportObject(this, true);
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void heartBeat() {
		// should do nothing
	}

	@Override
	public int getId() {
		return id;
	}

	@Override
	public void setId(int id) {
		this.id = id;
	}

	@Override
	public void createBlock(int blockId, String content) {
		createBlock(blockId, content.getBytes(Charset.forName("UTF-8")));
	}

	@Override
	public void createBlock(int blockId, byte[] content) {
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(new File(dir + blockId));
			fos.write(content);
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.blockIds.add(blockId);
	}

	@Override
	public void createBlock(int blockId, DataNode dataNode) {
		byte[] content = dataNode.fetchByteBlock(blockId);
		this.createBlock(blockId, content);
	}

	@Override
	public byte[] fetchByteBlock(int blockId) {
		try {
			File file = new File(dir + blockId);
			byte[] content = new byte[(int) file.length()];
			FileInputStream fis = new FileInputStream(file);
			fis.read(content);
			fis.close();
			return content;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String fetchStringBlock(int blockId) {
		byte[] bytes = this.fetchByteBlock(blockId);
		if (bytes == null) {
			return null;
		}
		String s = new String(bytes, Charset.forName("UTF-8"));
		return s;
	}

	@Override
	public String getFolder() {
		return this.dir;
	}

}
