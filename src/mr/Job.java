/**
 * 
 */
package mr;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * @author Nicolas_Yu
 *
 */
public class Job implements Serializable{

	private static final long serialVersionUID = -1452869710596489185L;
	
	Class<? extends Mapper> mapper = null;
	Class<? extends Reducer> reducer = null;
	
	Registry registry = null;
	
	String inputFilePath = null;
	String outputFilePath = null;
	
	JobTracker jobTracker = null;
	
	int map_ct = 0;
	int reduce_ct = 0;
	
	
	public Job(String host, int port) {
		try {
			registry = LocateRegistry.getRegistry(host, port);
			jobTracker = (JobTracker) registry.lookup("JobTracker");
			
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
