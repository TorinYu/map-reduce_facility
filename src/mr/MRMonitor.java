/**
 * 
 */
package mr;

/**
 * @author Nicolas_Yu
 *
 */
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

/**
 * Monitor of MapReducerFM
 * 
 * @author Nicolas_Yu
 */
public class MRMonitor {
	private String registryHost;
	private int registryPort;

	/**
	 * Constructor
	 * 
	 * @param registryHost
	 *            registry's host
	 * @param registryPort
	 *            registry's port
	 */
	public MRMonitor(String registryHost, int registryPort) {
		this.registryHost = registryHost;
		this.registryPort = registryPort;
	}

	/**
	 * Get the current status of a specific job
	 * 
	 * @param jobID
	 *            job's id
	 * @return string that describe the job's status
	 * @throws NotBoundException
	 * @throws RemoteException
	 */
	public String describe(String jobId) throws NotBoundException,
			RemoteException {
		Registry registry = LocateRegistry.getRegistry(registryHost,
				registryPort);
		JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
		return jobTracker.describeJob(jobId);
	}

	/**
	 * Get the current status of all jobs
	 * 
	 * @return string that describe the jobs' status
	 * @throws NotBoundException
	 * @throws RemoteException
	 */
	public String describe() throws NotBoundException, RemoteException {
		Registry registry = LocateRegistry.getRegistry(registryHost,
				registryPort);
		JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
		return jobTracker.describeJobs();
	}

	/**
	 * Terminate all jobs and JobTracker, TaskTrackers through RMI call
	 * 
	 * @throws NotBoundException
	 * @throws RemoteException
	 */
	public void terminate() throws NotBoundException, RemoteException {
		Registry registry = LocateRegistry.getRegistry(registryHost,
				registryPort);
		JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
		jobTracker.terminate();
	}

	/**
	 * Kill all jobs
	 * 
	 * @throws NotBoundException
	 * @throws RemoteException
	 */
	public void kill() throws NotBoundException, RemoteException {
		Registry registry = LocateRegistry.getRegistry(registryHost,
				registryPort);
		JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
		jobTracker.kill();
	}

	/**
	 * Kill a jobs with given job id
	 * 
	 * @param jobID
	 *            job's id
	 * @throws NotBoundException
	 * @throws RemoteException
	 */
	public void kill(String jobId) throws NotBoundException, RemoteException {
		Registry registry = LocateRegistry.getRegistry(registryHost,
				registryPort);
		JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
		jobTracker.kill(jobId);
	}

	private static void printUsage() {
		System.out.println("Usage:");
		System.out.println("describe jobs: describe");
		System.out.println("decribe one job: describe <JobId>");
		System.out.println("kill jobs: kill");
		System.out.println("kill one job: kill <JobId>");
		System.out.println("terminate MR: terminate");
		System.out.println("exit: exit");
	}

	/**
	 * Main method of MRMonitor
	 * 
	 * @param args
	 *            command-line arguments
	 */
	public static void main(String[] args) {
		String registryHost = args[0];
		int registryPort = Integer.parseInt(args[1]);
		MRMonitor monitor = new MRMonitor(registryHost, registryPort);

		printUsage();

		String input = "";
		Scanner scanner = new Scanner(System.in);
		while (true) {
			input = scanner.nextLine();
			String[] cmds = input.split(" ");
			int len = cmds.length;
			String type = cmds[0];
			switch (type) {
			case "describe":
				try {
					if (len == 2) {
						// monitor.describe(cmds[1]);
						System.out.println("d jobId");
					} else if (len == 1) {
						// monitor.describe();
						System.out.println("d");
					} else {
						printUsage();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;
			case "kill":
				try {
					if (len == 2) {
						monitor.kill(cmds[1]);
					} else if (len == 1) {
						monitor.kill();
					} else {
						printUsage();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;

			case "terminate":
				try {
					if (len == 1) {
						monitor.terminate();
					} else {
						printUsage();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return;
			case "exit":
				scanner.close();
				System.exit(0);
			default:
				printUsage();
				break;
			}
		}
	}
}
