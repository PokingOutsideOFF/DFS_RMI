import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

	static int regPort = Configurations.REG_PORT;

	static Registry registry ;

	static List<Client> clients = new ArrayList<>();

	/**
	 * respawns replica servers and register replicas at master
	 * @param master
	 * @throws IOException
	 */
	static void respawnReplicaServers(Master master)throws IOException{
		System.out.println("[@main] respawning replica servers ");
		// TODO make file names global
		BufferedReader br = new BufferedReader(new FileReader("repServers.txt"));
		int n = Integer.parseInt(br.readLine().trim());
		ReplicaLoc replicaLoc;
		String s;

		for (int i = 0; i < n; i++) {
			s = br.readLine().trim();
			replicaLoc = new ReplicaLoc(i, s.substring(0, s.indexOf(':')) , true);
			ReplicaServer rs = new ReplicaServer(i, "./"); 

			ReplicaInterface stub = (ReplicaInterface) UnicastRemoteObject.exportObject(rs, 0);
			registry.rebind("ReplicaClient"+i, stub);

			master.registerReplicaServer(replicaLoc, stub);

			System.out.println("replica server state [@ main] = "+rs.isAlive());
		}
		br.close();
	}

	public static void launchClients(){
		try {
			Scanner s = new Scanner(System.in);
			while(true){
				System.out.println("\nAre you an existing client? (yes/no) or Exit" );
				String choice = s.next();
				if (choice.equalsIgnoreCase("yes")) {
					System.out.println("Enter your client number:");
					int clientNumber = s.nextInt();
					if (clientNumber < 1 || clientNumber > clients.size()) {
						System.out.println("Invalid client number.");
						return;
					}
					clients.get(clientNumber - 1).performOperation();
				} 
				else if (choice.equalsIgnoreCase("no")) {
					Client newClient = new Client();
					clients.add(newClient);
					System.out.println("Client added as: Client " + (clients.lastIndexOf(newClient)));
					newClient.performOperation();
				} 
				else if(choice.equalsIgnoreCase("Exit")){
					System.out.println("Exiting.\n");
					return;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	static Master startMaster() throws AccessException, RemoteException{
		Master master = new Master();
		MasterServerClientInterface stub = 
				(MasterServerClientInterface) UnicastRemoteObject.exportObject(master, 0);
		registry.rebind("MasterServerClientInterface", stub);
		System.err.println("Server ready");
		return master;
	}

	public static void main(String[] args) throws IOException {

		try {
			LocateRegistry.createRegistry(regPort);
			registry = LocateRegistry.getRegistry(regPort);

			Master master = startMaster();
			respawnReplicaServers(master);
			launchClients();
			System.out.println("Bye");
			return;

		} catch (RemoteException   e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}

}
