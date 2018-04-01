package dbs.peer;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Hashtable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import dbs.peer.PeerUtility.ProtocolVersion;
import dbs.peer.PeerUtility.FileMetadata;
import dbs.peer.PeerUtility.ChunkMetadata;
import dbs.rmi.RemoteFunction;
import dbs.net.MulticastChannel;
import dbs.util.concurrent.LinkedTransientQueue;

import static dbs.peer.PeerUtility.METADATA_DIRECTORY;

public class Peer implements PeerInterface {

	/***************************************************************************************************
	***** Main *****************************************************************************************
	***************************************************************************************************/

	public static void main(String[] args) throws IOException {
		if (args.length != 9 || !(Pattern.matches(PeerUtility.PROTOCOL_VERSION_REGEX, args[0]) &&
								  Pattern.matches(PeerUtility.PEER_ID_REGEX, args[1]) &&
								  Pattern.matches(PeerUtility.ACCESS_POINT_REGEX, args[2]) &&
								  Pattern.matches(PeerUtility.ADDRESS_REGEX, args[3]) && Pattern.matches(PeerUtility.PORT_REGEX, args[4]) &&
								  Pattern.matches(PeerUtility.ADDRESS_REGEX, args[5]) && Pattern.matches(PeerUtility.PORT_REGEX, args[6]) &&
								  Pattern.matches(PeerUtility.ADDRESS_REGEX, args[7]) && Pattern.matches(PeerUtility.PORT_REGEX, args[8]))) {
			System.err.println("\n######## Distributed Backup Service ########" +
							   "\nPeer must be called with the following arguments:" +
							   "\n\t<protocol_version> <peer_id> <peer_ap> <MC_address> <MC_port> <MDB_address> <MDB_port> <MDR_address> <MDR_port>" +
							   "\n\tWhere:" +
							   "\n\t\t<protocol_version> is a sequence of the form <major>.<minor> where <major> and <minor> are single digits" +
							   "\n\t\t<peer_id> is a number between 0 and 999999999" +
							   "\n\t\t<peer_ap> is an identifier that can contain all lower non-zero-width ASCII characters except \"/\" (slash)" +
							   "\n\t\t<M*_address> is an IPv4 address" +
							   "\n\t\t<M*_port> is a number between 0 and 65535");
			System.exit(1);
		}

		Peer peer = new Peer(args[0], Integer.valueOf(args[1]), args[2],
		                     args[3], Integer.valueOf(args[4]),
		                     args[5], Integer.valueOf(args[6]),
		                     args[7], Integer.valueOf(args[8]));

		peer.run();
	}

	/***************************************************************************************************
	***** Constructor **********************************************************************************
	***************************************************************************************************/

	public Peer(String protocol_version, int id, String access_point,
	            String MC_address, int MC_port,
	            String MDB_address, int MDB_port,
	            String MDR_address, int MDR_port) throws IOException {
		System.out.println("\nInitializing peer...");

		/* Single-comment this line to switch to Cool-Mode™
		NetworkInterface net_int = MainInterface.find();
		if (net_int != null) {
			byte[] hw_addr = net_int.getHardwareAddress();
			this.ID = String.format("%02x%02x%02x%02x%02x%02x@",
									hw_addr[0], hw_addr[1], hw_addr[2],
									hw_addr[3], hw_addr[4], hw_addr[5])
							.concat(Long.toString(ProcessHandle.current().pid()));
		}
		else {
			System.err.println("\nFAILURE! Could not establish a connection through any available interface" +
			                   "\nDistributed Backup Service terminating...");
			System.exit(1);
			this.ID = null; // Placebo: So the compiler stops barking at us
		}
		/*/
		this.ID = Integer.toString(id);
		//*/

		PROTOCOL_VERSION = new ProtocolVersion(protocol_version);
		ACCESS_POINT = access_point;

		instances = new AtomicInteger(0);

		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		/*
		try (ObjectInputStream files_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + "files"));
		     ObjectInputStream local_chunks_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + "localchunks"));
		     ObjectInputStream remote_chunks_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + "remotechunks"))) {
			files_metadata = (Hashtable<String, FileMetadata>) files_stream.readObject();
			local_chunks_metadata = (Hashtable<String, ChunkMetadata>) local_chunks_stream.readObject();
			remote_chunks_metadata = (Hashtable<String, ChunkMetadata>) remote_chunks_stream.readObject();
		}
		catch (IOException | ClassNotFoundException e) {
			System.err.println("\nFAILURE! Couldn't load service metadata" +
			                   "\nDistributed Backup Service terminating...");
			System.exit(1);
		}
		/*/
		files_metadata = new Hashtable<>();
		local_chunks_metadata = new Hashtable<>();
		remote_chunks_metadata = new Hashtable<>();
		//*/
		backup_messages = new Hashtable<>();
		restore_messages = new Hashtable<>();

		MCsocket = new MulticastChannel(MC_address, MC_port);
		MDBsocket = new MulticastChannel(MDB_address, MDB_port);
		MDRsocket = new MulticastChannel(MDR_address, MDR_port);

		MCchannel = new PeerChannel(this, MCsocket);
		MDBchannel = new PeerChannel(this, MDBsocket);
		MDRchannel = new PeerChannel(this, MDRsocket);

		MCqueue = new PeerQueue(this, MCchannel);
		MDBqueue = new PeerQueue(this, MDBchannel);
		MDRqueue = new PeerQueue(this, MDRchannel);

		executor.execute(MCchannel);
		executor.execute(MDBchannel);
		executor.execute(MDRchannel);

		executor.execute(MCqueue);
		executor.execute(MDBqueue);
		executor.execute(MDRqueue);

		try {
			Registry registry;
			try { // Like a really weird if-else-statement
				registry = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
			}
			catch(RemoteException e) {
				registry = LocateRegistry.getRegistry(Registry.REGISTRY_PORT);
			}
			registry.bind(ACCESS_POINT, UnicastRemoteObject.exportObject(this, 0));
		}
		catch (AlreadyBoundException e) {
			UnicastRemoteObject.unexportObject(this, true);
			System.err.println("\nFAILURE! Access point \"" + ACCESS_POINT + "\" is already in use" +
			                   "\nDistributed Backup Service terminating...");
			System.exit(11);
		}

		System.out.println("\nPeer initialized with ID: " + ID +
		                   "\n\tProtocol version: " + PROTOCOL_VERSION +
		                   "\n\tAccess point: " + ACCESS_POINT);
	}

	/***************************************************************************************************
	***** Member constants *****************************************************************************
	***************************************************************************************************/

	final String ID;
	final ProtocolVersion PROTOCOL_VERSION;
	final String ACCESS_POINT;

	/***************************************************************************************************
	***** Member variables *****************************************************************************
	***************************************************************************************************/

	AtomicInteger instances;

	ThreadPoolExecutor executor;

	Hashtable<String, FileMetadata> files_metadata;
	Hashtable<String, ChunkMetadata> local_chunks_metadata;
	Hashtable<String, ChunkMetadata> remote_chunks_metadata;

	Hashtable<String, LinkedTransientQueue<byte[]>> backup_messages;
	Hashtable<String, LinkedTransientQueue<byte[]>> restore_messages;
	Hashtable<String, LinkedTransientQueue<byte[]>> reclaim_messages;

	MulticastChannel MCsocket;  // multicast control
	MulticastChannel MDBsocket; // multicast data backup
	MulticastChannel MDRsocket; // multicast data restore

	private PeerChannel MCchannel;
	private PeerChannel MDBchannel;
	private PeerChannel MDRchannel;

	private PeerQueue MCqueue;
	private PeerQueue MDBqueue;
	private PeerQueue MDRqueue;

	/***************************************************************************************************
	***** Member functions *****************************************************************************
	***************************************************************************************************/

	public void run() {
		// Subject to change
		synchronized (instances) {
			while (instances.get() >= 0) {
				try {
					instances.wait();
				} catch (InterruptedException e) {
					// Probably time to terminate
				}
			}
		}

		System.out.println("\nTerminating peer...");

		try {
			LocateRegistry.getRegistry(Registry.REGISTRY_PORT).unbind(ACCESS_POINT);
		}
		catch (RemoteException | NotBoundException e) {
			// That's weird, shouldn't happen
			e.printStackTrace();
		}

		try {
			UnicastRemoteObject.unexportObject(this, true);
		}
		catch (NoSuchObjectException e) {
			// That's weird, shouldn't... you guessed it
			e.printStackTrace();
		}

		MCchannel.stop();
		MDBchannel.stop();
		MDRchannel.stop();

		MCqueue.stop();
		MDBqueue.stop();
		MDRqueue.stop();

		executor.shutdown();
		/*
		boolean stored_metadata = true;
		try (ObjectOutputStream files_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + "files.new"));
		     ObjectOutputStream local_chunks_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + "localchunks.new"));
		     ObjectOutputStream remote_chunks_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + "remotechunks.new"))) {
			files_stream.writeObject(files_metadata);
			local_chunks_stream.writeObject(local_chunks_metadata);
			remote_chunks_stream.writeObject(remote_chunks_metadata);
		}
		catch (IOException e) {
			System.out.println("\nWARNING! Could not store updated metadata");
			stored_metadata = false;
		}

		if (stored_metadata) {
			new File(METADATA_DIRECTORY + "files.old").delete();
			new File(METADATA_DIRECTORY + "files").renameTo(new File(METADATA_DIRECTORY + "files.old"));
			new File(METADATA_DIRECTORY + "files.new").renameTo(new File(METADATA_DIRECTORY + "files"));

			new File(METADATA_DIRECTORY + "localchunks.old").delete();
			new File(METADATA_DIRECTORY + "localchunks").renameTo(new File(METADATA_DIRECTORY + "localchunks.old"));
			new File(METADATA_DIRECTORY + "localchunks.new").renameTo(new File(METADATA_DIRECTORY + "localchunks"));

			new File(METADATA_DIRECTORY + "remotechunks.old").delete();
			new File(METADATA_DIRECTORY + "remotechunks").renameTo(new File(METADATA_DIRECTORY + "remotechunks.old"));
			new File(METADATA_DIRECTORY + "remotechunks.new").renameTo(new File(METADATA_DIRECTORY + "remotechunks"));
		}
		//*/
		System.out.println("\nPeer terminated");
	}

	public void stop() {
		while (!instances.weakCompareAndSetPlain(0, Integer.MIN_VALUE));

		// Subject to change
		synchronized (instances) {
			instances.notifyAll();
		}
	}

	public String state() {
		// TODO
		return "";
	}

	public RemoteFunction backup(String filename, String fileID, byte[] file, int replication_degree) {
		return PeerProtocol.backup(this, filename, fileID, file, replication_degree);
	}

	public RemoteFunction restore(String filename) {
		return PeerProtocol.restore(this, filename);
	}

	public RemoteFunction delete(String filename) {
		return PeerProtocol.delete(this, filename);
	}

	public RemoteFunction reclaim(long disk_space) {
		return PeerProtocol.reclaim(this, disk_space);
	}
}
