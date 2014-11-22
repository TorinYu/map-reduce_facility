package dfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Map;

public class Downloader {

	private String registryHost;
	private int registryPort;

	public Downloader(String registryHost, int registryPort) {
		this.registryHost = registryHost;
		this.registryPort = registryPort;
	}

	public void download(String path, String fileName) {
		try {
			// try to create local file.
			if (path.contains("/")) {
				String dir = path.substring(0, path.lastIndexOf("/"));
				File folder = new File(dir);
				if (!folder.exists()) {
					folder.mkdirs();
				}
			}
			// create file output stream.
			FileOutputStream fos;

			fos = new FileOutputStream(new File(path));

			// locate registry
			Registry registry = LocateRegistry.getRegistry(this.registryHost,
					this.registryPort);
			NameNode namenode = (NameNode) registry.lookup("namenode");
			Map<Integer, List<Integer>> blocks = namenode
					.getAllBlocks(fileName);

			// we implement with tree map, which guarantees it to be ordered.
			for (Integer blockId : blocks.keySet()) {
				List<Integer> dataNodeIds = blocks.get(blockId);
				byte[] content = null;
				for (Integer id : dataNodeIds) {
					DataNode dataNode = namenode.fetchDataNode(id);
					try {
						content = dataNode.fetchByteBlock(blockId);
					} catch (Exception e) {
						continue;
					}
					break;
				}

				if (content == null) {
					fos.close();
					File file = new File(path);
					file.delete();
					throw new RemoteException("All Data Nodes with Block Id "
							+ blockId + "are dead!");
				}

				fos.write(content);
			}
			fos.close();

		} catch (FileNotFoundException e) {
			// e.printStackTrace();
			System.out.println("File Does Not Exist!");
		} catch (RemoteException e) {
			// e.printStackTrace();
			System.out.println("File Does Not Exist!");
		} catch (NotBoundException e) {
			// e.printStackTrace();
			System.out.println("Could not find registry!");
		} catch (IOException e) {
			// e.printStackTrace();
			System.out.println("There are I/O Exceptions!");
		}
	}

	public static void main(String args[]) {
		String registryHost = null;
		int registryPort = 0;
		String path = null;
		String filename = null;

		try {
			registryHost = args[0];
			registryPort = Integer.parseInt(args[1]);
			path = args[2];
			filename = args[3];
		} catch (Exception e) {
			System.out.println("Usage: <LocalPath> <DFSFileName>");
			System.exit(-1);
		}
		Downloader downloader = new Downloader(registryHost, registryPort);
		downloader.download(path, filename);
	}

}
