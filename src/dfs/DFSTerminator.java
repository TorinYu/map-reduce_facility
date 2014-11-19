package dfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DFSTerminator {
	public static void main(String args[]) {
		String registryHost = args[0];
		int registryPort = Integer.parseInt(args[1]);
		try {
			Registry registry = LocateRegistry.getRegistry(registryHost,
					registryPort);
			NameNode node = (NameNode) registry.lookup("namenode");
			node.terminate();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}finally{
			System.exit(0);
		}

	}
}
