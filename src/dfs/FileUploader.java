package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class FileUploader {

	private String registryHost;
	private int registryPort;

	public FileUploader(String registryHost, int registryPort) {
		this.registryHost = registryHost;
		this.registryPort = registryPort;
	}

	public static void main(String[] args) {
		String registryHost = null;
		int registryPort = 0;
		String path = null;
		String filename = null;
		int replicas = 0;

		try {
			registryHost = args[0];
			registryPort = Integer.parseInt(args[1]);
			path = args[2];
			replicas = Integer.parseInt(args[3]);
			filename = args[4];
		} catch (Exception e) {
			System.out.println("Usage: <LocalPath> <replicas> <DFSFileName>");
			System.exit(-1);
		}
		 FileUploader uploader = new FileUploader(registryHost, registryPort);
		 uploader.upload(path, replicas, filename);
		
	}

	public void upload(String filePath, int replicas, String alias) {

		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(
					filePath)));

			Registry registry = LocateRegistry.getRegistry(this.registryHost,
					this.registryPort);
			NameNode namenode = (NameNode) registry.lookup("namenode");
			FileInfo info = namenode.createFile(alias, replicas);
			if (info == null) {
				System.out.println("File Already Exists!");
				System.exit(-1);
			}
			String line = "";
			int size = namenode.getBlockSize();
			String content = "";
			int count = 0;
			while (line != null) {
				line = br.readLine();
				if (line != null
						&& count
								+ line.getBytes(Charset.forName("UTF-8")).length < size) {
					count += line.getBytes(Charset.forName("UTF-8")).length + 1;
					content += (line + "\n");
				} else {
					int blockId = namenode.getNextBlockId();
					for (int i = 0; i < info.getReplicas(); i++) {
						int id = namenode.allocateDataNode(blockId);
						DataNode node = namenode.fetchDataNode(id);
						node.createBlock(blockId, content);
						namenode.commitBlockAllocation(id, blockId);
					}
					info.getBlockIds().add(blockId);
					count = 0;
					content = "";
				}
			}
			namenode.updateFileInfos(alias, info);

		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
