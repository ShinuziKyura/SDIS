package dbs;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import static java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import dbs.PeerUtility.ProtocolVersion;
import dbs.PeerUtility.FileMetadata;
import dbs.PeerUtility.ChunkMetadata;
import nio.channels.MulticastChannel;
import rmi.RemoteFunction;
import util.concurrent.LinkedTransientQueue;

import static dbs.PeerUtility.*;

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
			System.exit(11);
		}

		Peer peer = new Peer(args[0], Integer.valueOf(args[1]), args[2],
		                     args[3], Integer.valueOf(args[4]),
		                     args[5], Integer.valueOf(args[6]),
		                     args[7], Integer.valueOf(args[8]));

		peer.run();
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

	private Condition condition;

	WriteLock exclusive_access;
	ReadLock shared_access;

	ConcurrentHashMap<String, FileMetadata> files_metadata;
	ConcurrentHashMap<String, ChunkMetadata> local_chunks_metadata;
	ConcurrentHashMap<String, ChunkMetadata> remote_chunks_metadata;

	AtomicLong storage_capacity;
	AtomicLong storage_usage;

	ConcurrentHashMap<String, LinkedTransientQueue<byte[]>> backup_messages;
	ConcurrentHashMap<String, LinkedTransientQueue<byte[]>> restore_messages;
	ConcurrentHashMap<String, LinkedTransientQueue<byte[]>> reclaim_messages;

	MulticastChannel MCchannel;
	MulticastChannel MDBchannel;
	MulticastChannel MDRchannel;

	PeerSender MCsender;
	PeerSender MDBsender;
	PeerSender MDRsender;

	private PeerReceiver MCreceiver;
	private PeerReceiver MDBreceiver;
	private PeerReceiver MDRreceiver;

	private PeerDispatcher MCdispatcher;
	private PeerDispatcher MDBdispatcher;
	private PeerDispatcher MDRdispatcher;

	PeerLogger logger;

	AtomicBoolean running;
	ThreadPoolExecutor executor;


	/***************************************************************************************************
	***** Constructor **********************************************************************************
	***************************************************************************************************/

	public Peer(String protocol_version, int id, String access_point,
	            String MC_address, int MC_port,
	            String MDB_address, int MDB_port,
	            String MDR_address, int MDR_port) throws IOException {
		System.out.println("\nInitializing peer...");
	
		ID = Integer.toString(id);
		PROTOCOL_VERSION = new ProtocolVersion(protocol_version);
		ACCESS_POINT = access_point;

		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		condition = lock.writeLock().newCondition();

		exclusive_access = lock.writeLock();
		shared_access = lock.readLock();

		METADATA_DIRECTORY = METADATA_DIRECTORY + ID + "/";
		DATA_DIRECTORY = DATA_DIRECTORY + ID + "/";

		File metadata = new File(METADATA_DIRECTORY);
		File data = new File(DATA_DIRECTORY);
		if(metadata.exists() && data.exists()) {
			String files = FILES + (Files.exists(Paths.get(METADATA_DIRECTORY + FILES + NEW)) ? NEW : "");
			String localchunks = LOCALCHUNKS + (Files.exists(Paths.get(METADATA_DIRECTORY + LOCALCHUNKS + NEW)) ? NEW : "");
			String remotechunks = REMOTECHUNKS + (Files.exists(Paths.get(METADATA_DIRECTORY + REMOTECHUNKS + NEW)) ? NEW : "");
			String storecap = STORECAP + (Files.exists(Paths.get(METADATA_DIRECTORY + STORECAP + NEW)) ? NEW : "");
			String storeuse = STOREUSE + (Files.exists(Paths.get(METADATA_DIRECTORY + STOREUSE + NEW)) ? NEW : "");

			try (ObjectInputStream files_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + files));
			     ObjectInputStream localchunks_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + localchunks));
			     ObjectInputStream remotechunks_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + remotechunks));
			     ObjectInputStream storecap_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + storecap));
			     ObjectInputStream storeuse_stream = new ObjectInputStream(new FileInputStream(METADATA_DIRECTORY + storeuse))) {
				files_metadata = (ConcurrentHashMap<String, FileMetadata>) files_stream.readObject();
				local_chunks_metadata = (ConcurrentHashMap<String, ChunkMetadata>) localchunks_stream.readObject();
				remote_chunks_metadata = (ConcurrentHashMap<String, ChunkMetadata>) remotechunks_stream.readObject();
				storage_capacity = (AtomicLong) storecap_stream.readObject();
				storage_usage = (AtomicLong) storeuse_stream.readObject();
			}
			catch (IOException | ClassNotFoundException e) {
				System.err.println("\nFAILURE! Could not load service metadata" +
				                   "\nDistributed Backup Service terminating...");
				System.exit(1);
			}
		}
		else if (!metadata.exists() && !data.exists()) {
			metadata.mkdirs();
			data.mkdirs();

			files_metadata = new ConcurrentHashMap<>();
			local_chunks_metadata = new ConcurrentHashMap<>();
			remote_chunks_metadata = new ConcurrentHashMap<>();
			storage_capacity = new AtomicLong(Long.MAX_VALUE);
			storage_usage = new AtomicLong(0);
		}
		else {
			System.err.println("\nFAILURE! Service files missing" +
			                   "\nDistributed Backup Service terminating...");
			System.exit(1);
		}

		backup_messages = new ConcurrentHashMap<>();
		restore_messages = new ConcurrentHashMap<>();
		reclaim_messages = new ConcurrentHashMap<>();

		MCchannel = new MulticastChannel(MC_address, MC_port);
		MDBchannel = new MulticastChannel(MDB_address, MDB_port);
		MDRchannel = new MulticastChannel(MDR_address, MDR_port);

		MCsender = new PeerSender(this, MCchannel);
		MDBsender = new PeerSender(this, MDBchannel);
		MDRsender = new PeerSender(this, MDRchannel);

		MCreceiver = new PeerReceiver(this, MCchannel);
		MDBreceiver = new PeerReceiver(this, MDBchannel);
		MDRreceiver = new PeerReceiver(this, MDRchannel);

		MCdispatcher = new PeerDispatcher(this, MCreceiver);
		MDBdispatcher = new PeerDispatcher(this, MDBreceiver);
		MDRdispatcher = new PeerDispatcher(this, MDRreceiver);

		logger = new PeerLogger(this);

		running = new AtomicBoolean(true);
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		executor.execute(MCsender);
		executor.execute(MDBsender);
		executor.execute(MDRsender);

		executor.execute(MCreceiver);
		executor.execute(MDBreceiver);
		executor.execute(MDRreceiver);

		executor.execute(MCdispatcher);
		executor.execute(MDBdispatcher);
		executor.execute(MDRdispatcher);

		executor.execute(logger);

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
			System.exit(12);
		}

		System.out.println("\nPeer initialized with ID: " + ID +
		                   "\n\tProtocol version: " + PROTOCOL_VERSION +
		                   "\n\tAccess point: " + ACCESS_POINT);
	}

	/***************************************************************************************************
	***** Member functions *****************************************************************************
	***************************************************************************************************/

	public void run() {
		exclusive_access.lock();

		while (running.get()) {
			try {
				condition.await(1, TimeUnit.MINUTES);
			}
			catch (InterruptedException e) {
				// Won't happen
			}

			logger.print("\nMain - Updating...");

			try (ObjectOutputStream files_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + FILES + NEW));
			     ObjectOutputStream localchunks_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + LOCALCHUNKS + NEW));
			     ObjectOutputStream remotechunks_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + REMOTECHUNKS + NEW));
			     ObjectOutputStream storecap_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + STORECAP + NEW));
			     ObjectOutputStream storeuse_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + STOREUSE + NEW))) {
				files_stream.writeObject(files_metadata);
				localchunks_stream.writeObject(local_chunks_metadata);
				remotechunks_stream.writeObject(remote_chunks_metadata);
				storecap_stream.writeObject(storage_capacity);
				storeuse_stream.writeObject(storage_usage);
			}
			catch (IOException e) {
				// Meh, we'll try again in a minute
			}
		}

		System.out.println("\nTerminating peer...");

		try {
			LocateRegistry.getRegistry(Registry.REGISTRY_PORT).unbind(ACCESS_POINT);
		}
		catch (RemoteException | NotBoundException e) {
			// That's weird, shouldn't happen
		}

		try {
			UnicastRemoteObject.unexportObject(this, false);
		}
		catch (NoSuchObjectException e) {
			// That's weird, shouldn't... you guessed it
		}

		try {
			MCchannel.close();
			MDBchannel.close();
			MDRchannel.close();
		}
		catch (IOException e) {
			// I sure hope nothing goes wrong...
		}

		MCsender.stop();
		MDBsender.stop();
		MDRsender.stop();

		MCreceiver.stop();
		MDBreceiver.stop();
		MDRreceiver.stop();

		logger.stop();

		executor.shutdown();
		//*
		boolean updated_metadata = true;
		try (ObjectOutputStream files_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + FILES + NEW));
		     ObjectOutputStream localchunks_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + LOCALCHUNKS + NEW));
		     ObjectOutputStream remotechunks_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + REMOTECHUNKS + NEW));
		     ObjectOutputStream storecap_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + STORECAP + NEW));
		     ObjectOutputStream storeuse_stream = new ObjectOutputStream(new FileOutputStream(METADATA_DIRECTORY + STOREUSE + NEW))) {
			files_stream.writeObject(files_metadata);
			localchunks_stream.writeObject(local_chunks_metadata);
			remotechunks_stream.writeObject(remote_chunks_metadata);
			storecap_stream.writeObject(storage_capacity);
			storeuse_stream.writeObject(storage_usage);
		}
		catch (IOException e) {
			System.out.println("\nWARNING! Could not store updated metadata");
			updated_metadata = false;
		}

		if (updated_metadata) {
			PeerUtility.updateFilenames(METADATA_DIRECTORY + FILES);
			PeerUtility.updateFilenames(METADATA_DIRECTORY + LOCALCHUNKS);
			PeerUtility.updateFilenames(METADATA_DIRECTORY + REMOTECHUNKS);
			PeerUtility.updateFilenames(METADATA_DIRECTORY + STORECAP);
			PeerUtility.updateFilenames(METADATA_DIRECTORY + STOREUSE);
		}
		//*/
		System.out.println("\nPeer terminated");

		exclusive_access.unlock();
	}

	public void stop() {
		exclusive_access.lock();
		running.set(false);
		condition.signal();
		exclusive_access.unlock();
	}

	public String state() {
		exclusive_access.lock();
		StringBuilder state = new StringBuilder("\nFiles:");
		for (Map.Entry<String, FileMetadata> file : files_metadata.entrySet()) {
			state.append("\n\tFile name: ").append(file.getKey())
			     .append("\n\t\tFile ID: ").append(file.getValue().fileID)
			     .append("\n\t\tChunk amount: ").append(file.getValue().chunk_amount)
			     .append("\n\t\tDesired replication degree: ").append(file.getValue().desired_replication);
			for (Map.Entry<String, ChunkMetadata> chunk : remote_chunks_metadata.entrySet()) {
				state.append("\n\t\t\tChunk ID: ").append(chunk.getKey())
				     .append("\n\t\t\t\tChunk size: ").append(chunk.getValue().chunk_size).append(" B")
				     .append("\n\t\t\t\tPerceived replication degree: ").append(chunk.getValue().perceived_replication.size());
			}
		}
		state.append("\nChunks:");
		for (Map.Entry<String, ChunkMetadata> chunk : local_chunks_metadata.entrySet()) {
			state.append("\n\tChunk ID: ").append(chunk.getKey())
			     .append("\n\t\tChunk size: ").append(chunk.getValue().chunk_size).append(" B")
			     .append("\n\t\tDesired replication degree: ").append(chunk.getValue().desired_replication)
			     .append("\n\t\tPerceived replication degree: ").append(chunk.getValue().perceived_replication.size());
		}
		Long capacity = storage_capacity.get();
		state.append("\nStorage capacity: ").append((capacity != Long.MAX_VALUE ? capacity.toString().concat(" B") : "No limit"))
		     .append("\nStorage usage: ").append(storage_usage.get()).append(" B");
		exclusive_access.unlock();
		return state.toString();
	}

	public RemoteFunction backup(String filename, String fileID, byte[] file, int replication_degree) {
		RemoteFunction result = null;
		switch (PROTOCOL_VERSION.toString()) {
			case "1.0":
			case "1.1":
				shared_access.lock();
				result = (running.get() ?
				          PeerProtocol.initiator_backup(this, filename, fileID, file, replication_degree) :
				          PeerProtocol.failure(FailureType.TERM_SERVICE));
				shared_access.unlock();
				break;
		}
		return result;
	}

	public RemoteFunction backup_1(String filename, String fileID, byte[] file, int replication_degree) {
		RemoteFunction result = null;
		switch (PROTOCOL_VERSION.toString()) {
			case "1.0":
				result = PeerProtocol.failure(FailureType.INCOMPAT_VERSION);
				break;
			case "1.1":
				shared_access.lock();
				result = (running.get() ?
				          PeerProtocol.initiator_backup_1(this, filename, fileID, file, replication_degree) :
				          PeerProtocol.failure(FailureType.TERM_SERVICE));
				shared_access.unlock();
				break;
		}
		return result;
	}

	public RemoteFunction restore(String filename) {
		RemoteFunction result = null;
		switch (PROTOCOL_VERSION.toString()) {
			case "1.0":
			case "1.1":
				shared_access.lock();
				result = (running.get() ?
				          PeerProtocol.initiator_restore(this, filename) :
				          PeerProtocol.failure(FailureType.TERM_SERVICE));
				shared_access.unlock();
				break;
		}
		return result;
	}

	public RemoteFunction restore_1(String filename) {
		RemoteFunction result = null;
		switch (PROTOCOL_VERSION.toString()) {
			case "1.0":
				result = PeerProtocol.failure(FailureType.INCOMPAT_VERSION);
				break;
			case "1.1":
				exclusive_access.lock();
				result = (running.get() ?
				          PeerProtocol.initiator_restore_1(this, filename) :
				          PeerProtocol.failure(FailureType.TERM_SERVICE));
				exclusive_access.unlock();
				break;
		}
		return result;
	}

	public RemoteFunction delete(String filename) {
		RemoteFunction result = null;
		switch (PROTOCOL_VERSION.toString()) {
			case "1.0":
			case "1.1":
				shared_access.lock();
				result = (running.get() ?
				          PeerProtocol.initiator_delete(this, filename) :
				          PeerProtocol.failure(FailureType.TERM_SERVICE));
				shared_access.unlock();
				break;
		}
		return result;
	}

	public RemoteFunction reclaim(long disk_space) {
		RemoteFunction result = null;
		switch (PROTOCOL_VERSION.toString()) {
			case "1.0":
			case "1.1":
				shared_access.lock();
				result = (running.get() ?
				          PeerProtocol.initiator_reclaim(this, disk_space) :
				          PeerProtocol.failure(FailureType.TERM_SERVICE));
				shared_access.unlock();
				break;
		}
		return result;
	}
}
