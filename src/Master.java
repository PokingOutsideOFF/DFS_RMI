import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
 


public class Master implements MasterReplicaInterface, MasterServerClientInterface, Remote{

	class HeartBeatTask extends TimerTask {

		@Override
		public void run() {
			// check state of replicas
			for (ReplicaLoc replicaLoc : replicaServersLocs) {
				try {
					replicaServersStubs.get(replicaLoc.getId()).isAlive();
				} catch (RemoteException e) {
					replicaLoc.setAlive(false);
					e.printStackTrace();
				}
			}
		}
	}
	
	private int nextTID;
	private int heartBeatRate = Configurations.HEART_BEAT_RATE;
	private int replicationN = Configurations.REPLICATION_N; // number of file replicas
	private Timer HeartBeatTimer;
	private Random randomGen;

	private Map<String,	 List<ReplicaLoc> > filesLocationMap;
	private Map<String,	 ReplicaLoc> primaryReplicaMap;
	//	private Map<Integer, String> activeTransactions; // active transactions <ID, fileName>
	private List<ReplicaLoc> replicaServersLocs;
	private List<ReplicaMasterInterface> replicaServersStubs; 


	public Master() {
		filesLocationMap = new HashMap<String, List<ReplicaLoc>>();
		primaryReplicaMap = new HashMap<String, ReplicaLoc>();
//		activeTransactions = new HashMap<Integer, String>();
		replicaServersLocs = new ArrayList<ReplicaLoc>();
		replicaServersStubs = new ArrayList<ReplicaMasterInterface>();

		nextTID = 0;
		randomGen = new Random();
		
		HeartBeatTimer = new Timer();  //At this line a new Thread will be created
		HeartBeatTimer.scheduleAtFixedRate(new HeartBeatTask(), 0, heartBeatRate); //delay in milliseconds
	}

	/**
	 * elects a new primary replica for the given file
	 * @param fileName
	 */
	private void assignNewMaster(String fileName){
		List<ReplicaLoc> replicas = filesLocationMap.get(fileName);
		boolean newPrimaryAssigned = false;
		for (ReplicaLoc replicaLoc : replicas) {
			if (replicaLoc.isAlive()){
				newPrimaryAssigned = true;
				primaryReplicaMap.put(fileName, replicaLoc);
				try {
					replicaServersStubs.get(replicaLoc.getId()).takeCharge(fileName, filesLocationMap.get(fileName));
				} catch (RemoteException | NotBoundException e) {
					e.printStackTrace();
				}
				break;
			}
		}

		if (!newPrimaryAssigned){
			//TODO a7a ya3ni
		}
	}

	/**
	 * creates a new file @ N replica servers that are randomly chosen
*/
	private void createNewFile(String fileName) {
		System.out.println("[@Master] Creating new file initiated");
		List<ReplicaLoc> replicas = new ArrayList<>();

		// Randomly select three unique replicas
		Set<Integer> chosenReplicas = new HashSet<>();
		while (chosenReplicas.size() < replicationN) {
			int randomReplica = randomGen.nextInt(replicaServersLocs.size());
			chosenReplicas.add(randomReplica);
		}

		// Create the file on selected replicas
		for (int replicaIndex : chosenReplicas) {
			try {
				replicaServersStubs.get(replicaIndex).createFile(fileName);
				replicas.add(replicaServersLocs.get(replicaIndex));
			} catch (IOException e) {
				// Failed to create the file at replica server
				e.printStackTrace();
			}
		}

		// Designate the primary replica
		int primaryIndex = chosenReplicas.iterator().next();
		try {
			replicaServersStubs.get(primaryIndex).takeCharge(fileName, replicas);
		} catch (RemoteException | NotBoundException e) {
			// Couldn't assign the master replica
			e.printStackTrace();
		}

		filesLocationMap.put(fileName, replicas);
		primaryReplicaMap.put(fileName, replicaServersLocs.get(primaryIndex));
	}

	
	@Override
	public List<ReplicaLoc> read(String fileName) throws FileNotFoundException,
	IOException, RemoteException {
		List<ReplicaLoc> replicaLocs = filesLocationMap.get(fileName);
		if (replicaLocs == null)
			throw new FileNotFoundException();
		return replicaLocs;
	}

	@Override
	public WriteAck write(String fileName) throws RemoteException, IOException {
		System.out.println("[@Master] write request initiated");
		long timeStamp = System.currentTimeMillis();

		List<ReplicaLoc> replicaLocs= filesLocationMap.get(fileName);
		int tid = nextTID++;
		if (replicaLocs == null)	// file not found
			createNewFile(fileName);

		ReplicaLoc primaryReplicaLoc = primaryReplicaMap.get(fileName);

		if (primaryReplicaLoc == null)
			throw new IllegalStateException("No primary replica found");

		// if the primary replica is down .. elect a new replica
		if (!primaryReplicaLoc.isAlive()){
			assignNewMaster(fileName);
			primaryReplicaLoc = primaryReplicaMap.get(fileName);
		}

		return new WriteAck(tid, timeStamp,primaryReplicaLoc);
	}

	@Override
	public ReplicaLoc locatePrimaryReplica(String fileName)
			throws RemoteException {
		
		return primaryReplicaMap.get(fileName);
	}
	

	/**
	 * registers new replica server @ the master by adding required meta data
	 * @param replicaLoc
	 * @param replicaStub
	 */
	public void registerReplicaServer(ReplicaLoc replicaLoc, ReplicaInterface replicaStub){
		replicaServersLocs.add(replicaLoc);
		replicaServersStubs.add( (ReplicaMasterInterface) replicaStub);
	}

	

	//TODO add commit to master to handle meta-data

}
