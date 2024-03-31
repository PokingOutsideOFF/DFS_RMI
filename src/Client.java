import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalDateTime;
import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Client {

	private int clientNumber;
    private List<String> operationHistory;

    public Client(int clientNumber) {
        this.clientNumber = clientNumber;
        this.operationHistory = new ArrayList<>();
    }


	MasterServerClientInterface masterStub;
	static Registry registry;
	int regPort = Configurations.REG_PORT;
	String regAddr = Configurations.REG_ADDR;
	int chunkSize = Configurations.CHUNK_SIZE; // in bytes 
	
	public Client() {
		try {
			registry = LocateRegistry.getRegistry(regAddr, regPort);
			masterStub =  (MasterServerClientInterface) registry.lookup("MasterServerClientInterface");
			System.out.println("[@client] Master Stub fetched successfuly");
		} catch (RemoteException | NotBoundException e) {
			// fatal error .. no registry could be linked
			e.printStackTrace();
		}
	}

	public void performOperation() throws MessageNotFoundException {
        // Perform operations...
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        boolean continueOperations = true;
		try{
			
			while (continueOperations) {
				System.out.println("\nChoose an operation:");
				System.out.println("1. Read");
				System.out.println("2. Write");
				System.out.println("3. Exit");
				int choice = Integer.parseInt(reader.readLine());
				
				switch (choice) {
					case 1:
						System.out.println("Enter the filename to read:");
						String filename = reader.readLine();
						byte[] data = read(filename);
						System.out.println("Content of " + filename + ": " + new String(data));
						break;
					case 2:
						System.out.println("Enter the filename to write:");
						String writeFilename = reader.readLine();
						System.out.println("Enter the data to write:");
						String inputData = reader.readLine().trim();

						byte[] writeData = inputData.getBytes(StandardCharsets.UTF_8);
						write(writeFilename, writeData);
						System.out.println("Data written successfully to " + writeFilename);
						break;
					case 3:
						continueOperations = false;
						break;
					default:
                    	System.out.println("Invalid choice. Please choose 1, 2, or 3.");
            	}
				String a = choice > 1 ? choice > 2 ? "Exit" : "Write" : "Read";
				logOperation("Performed "+ a +" operation");
			}

		} catch (NotBoundException | IOException  e) {
			e.printStackTrace();
		}
        // Log the operation along with timestamp
        
    }

	// public byte[] read(String fileName) throws IOException, NotBoundException{
	// 	// try{

	// 	// 	List<ReplicaLoc> locations = masterStub.read(fileName);
	// 	// 	System.out.println("[@client] Master Granted read operation");
			
	// 	// 	// TODO fetch from all and verify 
	// 	// 	ReplicaLoc replicaLoc = locations.get(0);

	// 	// 	ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
	// 	// 	FileContent fileContent = replicaStub.read(fileName);
	// 	// 	System.out.println("[@client] read operation completed successfuly");
	// 	// 	System.out.println("[@client] data:");
			
	// 	// 	System.out.println(new String(fileContent.getData()));
	// 	// 	return fileContent.getData();
	// 	// } catch(Exception e){
	// 	// 	e.printStackTrace();
	// 	// 	return new byte[0];
	// 	// }
	
	// 	try {
	// 		List<ReplicaLoc> locations = masterStub.read(fileName);
	// 		System.out.println("[@client] Master granted read operation");

	// 		// Try to read from all replicas until the file is found
	// 		for (ReplicaLoc replicaLoc : locations) {
	// 			try {
	// 				ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
	// 				FileContent fileContent = replicaStub.read(fileName);
	// 				System.out.println("[@client] Read operation completed successfully from Replica " + replicaLoc.getId());
	// 				System.out.println("[@client] Data:");
	// 				System.out.println(new String(fileContent.getData()));
	// 				return fileContent.getData();
	// 			} catch (Exception e) {
	// 				// If an error occurs, continue to the next replica
	// 				System.err.println("[@client] Error reading from Replica " + replicaLoc.getId() + ": " + e.getMessage());
	// 			}
	// 		}

	// 		// If no replica has the file, throw an error
	// 		throw new FileNotFoundException("File not found in any replica");
	// 	} catch (Exception e) {
	// 		e.printStackTrace();
	// 		return new byte[0];	
	// 	}
	// }
	
	public byte[] read(String fileName) throws IOException, NotBoundException {
    try {
        List<ReplicaLoc> locations = masterStub.read(fileName);
        System.out.println("[@client] Master granted read operation");

        // Try to read from all replicas concurrently until the file is found
        ExecutorService executor = Executors.newFixedThreadPool(locations.size());
        List<Future<FileContent>> futures = new ArrayList<>();

        for (ReplicaLoc replicaLoc : locations) {
            futures.add(executor.submit(() -> {
                try {
                    ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + replicaLoc.getId());
                    return replicaStub.read(fileName);
                } catch (Exception e) {
                    // Log the error and return null
                    System.err.println("[@client] Error reading from Replica " + replicaLoc.getId() + ": " + e.getMessage());
                    return null;
                }
            }));
        }

        // Wait for any future to complete and return the file content
        for (Future<FileContent> future : futures) {
            try {
                FileContent fileContent = future.get();
                if (fileContent != null) {
                    System.out.println("[@client] Read operation completed successfully");
                    System.out.println("[@client] Data:");
                    System.out.println(new String(fileContent.getData()));
                    return fileContent.getData();
                }
            } catch (InterruptedException | ExecutionException e) {
                // Log the error and continue
                System.err.println("[@client] Error occurred while waiting for read operation: " + e.getMessage());
            }
        }

        // If no replica has the file, throw a FileNotFoundException


        throw new FileNotFoundException("File not found in any replica");
    } catch (Exception e) {
        System.out.println(e);
        return new byte[0];
    }
}

	public ReplicaServerClientInterface initWrite(String fileName, Long txnID) throws IOException, NotBoundException{
		WriteAck ackMsg = masterStub.write(fileName);
		txnID = new Long(ackMsg.getTransactionId());
		return (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());
	}
	
	public void writeChunk (long txnID, String fileName, byte[] chunk, long seqN, ReplicaServerClientInterface replicaStub) throws RemoteException, IOException{
		
		FileContent fileContent = new FileContent(fileName, chunk);
		ChunkAck chunkAck;
		
		do { 
			chunkAck = replicaStub.write(txnID, seqN, fileContent);
		} while(chunkAck.getSeqNo() != seqN);
	}


	public void write(String fileName, byte[] data) throws IOException, NotBoundException, MessageNotFoundException {
		WriteAck ackMsg = masterStub.write(fileName);
		ReplicaServerClientInterface replicaStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient" + ackMsg.getLoc().getId());

		System.out.println("[@client] Master granted write operation");

		int chunkSize = Configurations.CHUNK_SIZE;
		int numChunks = (int) Math.ceil((double) data.length / chunkSize);

		for (int i = 0; i < numChunks; i++) {
			int offset = i * chunkSize;
			int length = Math.min(chunkSize, data.length - offset);
			byte[] chunk = new byte[length];
			System.arraycopy(data, offset, chunk, 0, length);
			FileContent fileContent = new FileContent(fileName, chunk);

			// Write the chunk to the replica
			try {
				ChunkAck chunkAck = replicaStub.write(ackMsg.getTransactionId(), i, fileContent);
				// Check if the chunk was written successfully
				if (chunkAck.getSeqNo() != i) {
					// Handle write error
					System.err.println("[@client] Error writing chunk " + i);
					return;
				}
			} catch (Exception e) {
				// Handle write error
				System.err.println("[@client] Error writing chunk " + i + ": " + e.getMessage());
				return;
			}
		}

		System.out.println("[@client] Write operation complete");

		// Commit the transaction
		ReplicaLoc primaryLoc = masterStub.locatePrimaryReplica(fileName);
		replicaStub.commit(ackMsg.getTransactionId(), numChunks);
		System.out.println("[@client] Commit operation complete");
		System.out.println("\nPrimary Replica is: Replica " + primaryLoc.getId() + " \n");
	}
	
	private void logOperation(String operation) {
        String logEntry = String.format("[%s] Client %d: %s", LocalDateTime.now(), clientNumber, operation);
        System.out.println(logEntry);
		// this.operationHistory.add(logEntry);
    }
	
	public void commit(String fileName, long txnID, long seqN) throws MessageNotFoundException, IOException, NotBoundException{
		ReplicaLoc primaryLoc = masterStub.locatePrimaryReplica(fileName);
		ReplicaServerClientInterface primaryStub = (ReplicaServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(txnID, seqN);
		System.out.println(primaryLoc);
		System.out.println("[@client] commit operation complete");
	}
	
	public void batchOperations(String[] cmds){
		System.out.println("[@client] batch operations started");
		String cmd ;
		String[] tokens;
		for (int i = 0; i < cmds.length; i++) {
			cmd = cmds[i];
			tokens = cmd.split(", ");
			try {
				if (tokens[0].trim().equals("read"))
					this.read(tokens[1].trim());
				else if (tokens[0].trim().equals("write"))
					this.write(tokens[1].trim(), tokens[2].trim().getBytes());
				else if (tokens[0].trim().equals("commit"))
						this.commit(tokens[1].trim(), Long.parseLong(tokens[2].trim()), Long.parseLong(tokens[3].trim()));
			}catch (IOException | NotBoundException | MessageNotFoundException e){
				System.err.println("Operation "+i+" Failed");
			}
		}
		System.out.println("[@client] batch operations completed");
	}
	
}
