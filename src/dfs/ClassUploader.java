package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class ClassUploader {
	private String registryHost;
	private int registryPort;

	public ClassUploader(String registryHost, int registryPort) {
		this.registryHost = registryHost;
		this.registryPort = registryPort;
	}

	public void upload(String path, int replicas, String alias) {
		/*
		 * Finish read the file.
		 */

		try {
			File file = new File(path);
			FileInputStream fileInputStream = new FileInputStream(file);
			byte[] content = new byte[(int) file.length()];
			fileInputStream.read(content);
			fileInputStream.close();
			//finish the file read.
			
			Registry registry = LocateRegistry.getRegistry(this.registryHost,
					this.registryPort);
			NameNode namenode = (NameNode) registry.lookup("namenode");
			FileInfo info = namenode.createFile(alias, replicas);
			if (info == null) {
				System.out.println("File Already Exists!");
				System.exit(-1);
			}
			int blockId = namenode.getNextBlockId();
			for (int i = 0; i < info.getReplicas(); i++) {
				int id = namenode.allocateDataNode(blockId);
				DataNode node = namenode.fetchDataNode(id);
				node.createBlock(blockId, content);
				namenode.commitBlockAllocation(id, blockId);
			}
			info.getBlockIds().add(blockId);
			namenode.updateFileInfos(alias, info);

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		String registryHost = null;
		int registryPort = 0;
		String path = null;
		int replicas = 0;
		String alias = null;

		try {
			registryHost = args[0];
			registryPort = Integer.parseInt(args[1]);
			path = args[2];
			replicas = Integer.parseInt(args[3]);
			alias = args[4];
		} catch (Exception e) {
			System.out.println("Usage: <FilePath> <Replicas> <DFSFileName>");
			System.exit(-1);
		}
		ClassUploader uploader = new ClassUploader(registryHost, registryPort);
		uploader.upload(path, replicas, alias);

	}
}
