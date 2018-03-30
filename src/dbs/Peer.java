package dbs;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.Hashtable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import rmi.RMIResult;
import net.MulticastChannel;
import util.GenericArrays;
import util.concurrent.LinkedTransientQueue;

import static dbs.PeerUtility.*;

public class Peer implements PeerInterface {
	/*
		Main
	 */
	public static void main(String[] args) throws PeerException, IOException {
		//* Single-comment this line to activate the check
		if (args.length != 9 || !(Pattern.matches(PROTOCOL_VERSION_REGEX, args[0]) &&
								  Pattern.matches(PEER_ID_REGEX, args[1]) &&
								  Pattern.matches(ACCESS_POINT_REGEX, args[2]) &&
								  Pattern.matches(ADDRESS_REGEX, args[3]) && Pattern.matches(PORT_REGEX, args[4]) &&
								  Pattern.matches(ADDRESS_REGEX, args[5]) && Pattern.matches(PORT_REGEX, args[6]) &&
								  Pattern.matches(ADDRESS_REGEX, args[7]) && Pattern.matches(PORT_REGEX, args[8]))) /**/ {
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
	/*	Peer peer = new Peer("1.0", 1,"DBS_TEST",
							 "225.0.0.0", 8000,
							 "225.0.0.0", 8001,
							 "225.0.0.0", 8002); */

		peer.run();
	}

	/*
		Constants
	 */
	private static final long MAXIMUM_FILE_SIZE = 63999999999L;
	private static final int MAXIMUM_CHUNK_SIZE = 64000;

	/*
		Member constants
	 */
	final String ID;
	final ProtocolVersion PROTOCOL_VERSION;
	final String ACCESS_POINT;

	/*
		Member variables
	 */
	AtomicInteger processes;
	AtomicBoolean running;

	Random generator;
	ThreadPoolExecutor executor;

	Hashtable<String, String> stored_files; // Read from file
	Hashtable<String, String> stored_chunks;

	Hashtable<String, LinkedTransientQueue<byte[]>> DBMessages;

	MulticastChannel MCSocket;  // multicast control
	MulticastChannel MDBSocket; // multicast data backup
	MulticastChannel MDRSocket; // multicast data restore

	private PeerChannel MCChannel;
	private PeerChannel MDBChannel;
	private PeerChannel MDRChannel;

	private PeerQueue MCQueue;
	private PeerQueue MDBQueue;
	private PeerQueue MDRQueue;

	/*
		Constructor
	 */
	public Peer(String protocol_version, int id, String access_point,
				String MC_address, int MC_port,
				String MDB_address, int MDB_port,
				String MDR_address, int MDR_port) throws PeerException, IOException {
		System.out.println("\nInitializing peer...");

		/* Single-comment this line to switch to Cool-Mode
		NetworkInterface net_int = PeerUtility.mainInterface();
		if (net_int != null) {
			byte[] hw_addr = net_int.getHardwareAddress();
			this.ID = String.format("%02x%02x%02x%02x%02x%02x@",
									hw_addr[0], hw_addr[1], hw_addr[2],
									hw_addr[3], hw_addr[4], hw_addr[5])
							.concat(Long.toString(ProcessHandle.current().pid()));
		}
		else {
			throw new PeerException("Could not establish a connection through any available interface" +
									" - Distributed Backup Service unavailable");
		}
		/*/
		this.ID = Integer.toString(id);
		//*/

		PROTOCOL_VERSION = new ProtocolVersion(protocol_version);
		ACCESS_POINT = access_point;

		processes = new AtomicInteger(0);
		running = new AtomicBoolean(true);

		generator = new Random(ProcessHandle.current().pid());
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		try (FileInputStream files_stream = new FileInputStream("src/dbs/data/files.data");
		     FileInputStream chunks_stream = new FileInputStream("src/dbs/data/chunks.data")) {

			ObjectInputStream objectstream = new ObjectInputStream(files_stream);
			stored_files = (Hashtable<String, String>) objectstream.readObject();
			objectstream.close();

			objectstream = new ObjectInputStream(chunks_stream);
			stored_chunks = (Hashtable<String, String>) objectstream.readObject();
			objectstream.close();
		}
		catch (IOException | ClassNotFoundException e) {
			// What should we do?
		}

		DBMessages = new Hashtable<>();

		MCSocket = new MulticastChannel(MC_address, MC_port);
		MDBSocket = new MulticastChannel(MDB_address, MDB_port);
		MDRSocket = new MulticastChannel(MDR_address, MDR_port);

		MCChannel = new PeerChannel(this, MCSocket);
		MDBChannel = new PeerChannel(this, MDBSocket);
		MDRChannel = new PeerChannel(this, MDRSocket);

		MCQueue = new PeerQueue(this, MCChannel.queue());
		MDBQueue = new PeerQueue(this, MDBChannel.queue());
		MDRQueue = new PeerQueue(this, MDRChannel.queue());

		executor.execute(MCChannel);
		executor.execute(MDBChannel);
		executor.execute(MDRChannel);

		executor.execute(MCQueue);
		executor.execute(MDBQueue);
		executor.execute(MDRQueue);

		try {
			try {
				LocateRegistry.createRegistry(Registry.REGISTRY_PORT)
							  .bind(ACCESS_POINT, UnicastRemoteObject.exportObject(this, 0));
			}
			catch(RemoteException e) {
				LocateRegistry.getRegistry(Registry.REGISTRY_PORT)
							  .bind(ACCESS_POINT, UnicastRemoteObject.exportObject(this, 0));
			}
		}
		catch (AlreadyBoundException e) {
			UnicastRemoteObject.unexportObject(this, true);
			throw new PeerException("Access point \"" + ACCESS_POINT + "\" is already in use");
		}

		System.out.println("\nPeer initialized with ID: " + ID +
		                   "\n\tProtocol version: " + PROTOCOL_VERSION +
		                   "\n\tAccess point: " + ACCESS_POINT);
	}

	/****************************************************************************************************
	 *** Member functions *******************************************************************************
	 ****************************************************************************************************/
	public void run() {
		// This will be removed
		synchronized (running) {
			while (running.get()) {
				try {
					running.wait();
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
			UnicastRemoteObject.unexportObject(this, false);
		}
		catch (NoSuchObjectException e) {
			// That's weird, shouldn't... you guessed it
			e.printStackTrace();
		}

		MCChannel.stop();
		MDBChannel.stop();
		MDRChannel.stop();

		MCQueue.stop();
		MDBQueue.stop();
		MDRQueue.stop();

		executor.shutdown();

		try (FileOutputStream files_stream = new FileOutputStream("src/dbs/data/files.new.data");
		     FileOutputStream chunks_stream = new FileOutputStream("src/dbs/data/chunks.new.data")) {

			ObjectOutputStream objectstream = new ObjectOutputStream(files_stream);
			objectstream.writeObject(stored_files);
			objectstream.close();

			objectstream = new ObjectOutputStream(chunks_stream);
			objectstream.writeObject(stored_chunks);
			objectstream.close();
		}
		catch (IOException e) {
			// What should we do?
		}

		try {
			Files.delete(Paths.get("src/dbs/data/files.old.data"));
			File old_files = new File("src/dbs/data/files.data");
			old_files.renameTo(new File("src/dbs/data/files.old.data"));
			File new_files = new File("src/dbs/data/files.new.data");
			new_files.renameTo(new File("src/dbs/data/files.data"));

			Files.delete(Paths.get("src/dbs/data/chunks.old.data"));
			File old_chunks = new File("src/dbs/data/chunks.data");
			old_chunks.renameTo(new File("src/dbs/data/chunks.old.data"));
			File new_chunks = new File("src/dbs/data/chunks.new.data");
			new_chunks.renameTo(new File("src/dbs/data/chunks.data"));
		}
		catch (IOException e) {
			// What should we do?
		}

		System.out.println("\nPeer terminated");
	}

	public void stop() {
		while (!processes.weakCompareAndSetPlain(0, Integer.MIN_VALUE));
		running.set(false);

		// This will be removed
		synchronized (running) {
			running.notifyAll();
		}
	}

	public RMIResult backup(String filename, String fileID, byte[] file, int replication_degree) {
		if (processes.getAndIncrement() < 0) {
			processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! Peer process terminating...");
				return 1;
			});
		}
		if (DBMessages.containsKey(fileID)) {
			processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! Instance of BACKUP protocol for this fileID already exists" +
				                   "\nBACKUP protocol terminating...");
				return 2;
			});
		}

		if (file.length == 0 || file.length > MAXIMUM_FILE_SIZE) {
			processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! File must be greater than 0 bytes and less than 64 gigabytes" +
				                   "\nBACKUP protocol terminating...");
				return 11;
			});
		}
		if (replication_degree < 1 || replication_degree > 9) {
			processes.decrementAndGet();
			return new RMIResult<>((args) -> {
				System.err.println("\nERROR! Replication degree must be greater than 0 and less than 10" +
				                   "\nBACKUP protocol terminating...");
				return 12;
			});
		}

		int stored;
		int requests;
		LinkedTransientQueue<byte[]> replies = new LinkedTransientQueue<>();
		DBMessages.put(fileID, replies);

		String old_fileID = stored_files.put(filename, fileID);
		if (old_fileID != null) {
			executor.execute(new PeerProtocol(this, PeerUtility.generateProtocolHeader(
					MessageType.DELETE, PROTOCOL_VERSION, ID, old_fileID,null, null)));
		}

		int chunk_count = 0;
		int chunk_amount = file.length / MAXIMUM_CHUNK_SIZE + 1;
		do {
			int chunk_size = (chunk_count + 1) * MAXIMUM_CHUNK_SIZE < file.length ? (chunk_count + 1) * MAXIMUM_CHUNK_SIZE : file.length;
			byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, PROTOCOL_VERSION, ID, fileID, chunk_count, replication_degree);
			byte[] chunk_body = Arrays.copyOfRange(file, chunk_count * MAXIMUM_CHUNK_SIZE, chunk_size);
			byte[] chunk = GenericArrays.join(chunk_header, chunk_body);

			stored = 0;
			requests = 0;
			while (stored < replication_degree && requests < 5) {
				try {
					MDBSocket.send(chunk);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				replies.init((1 << requests++), TimeUnit.SECONDS);
				while (replies.take() != null) {
					++stored;
				}
			}

			if (stored > 0 && stored < replication_degree) {
				System.out.println("\nWARNING! Replication degree could not be met:" +
				                   "\n\tRequested - " + replication_degree +
				                   "\n\tActual - " + stored);
			}
		} while (++chunk_count < chunk_amount && stored != 0);

		if (stored == 0 && chunk_count != 0) {
			executor.execute(new PeerProtocol(this, PeerUtility.generateProtocolHeader(
					MessageType.DELETE, PROTOCOL_VERSION, ID, stored_files.remove(filename), null, null)));
		}

		DBMessages.remove(fileID);

		processes.decrementAndGet();

		return (stored != 0 ?
		        new RMIResult<>((args) -> {
		        	return 0;
		        }) :
		        new RMIResult<>((args) -> {
		        	System.err.println("\nERROR! Chunk " + args[0] + " could not be stored" +
			                           "\nBACKUP protocol terminating...");
		        	return 21;
		        }, new String[]{ Integer.toString(chunk_count) }));
	}

	public RMIResult restore(String pathname) {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}

	public RMIResult delete(String pathname) {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}

	public RMIResult reclaim(int disk_space) {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}

	public RMIResult state() {
		// TODO
		return new RMIResult<>((args) -> {
			return 0;
		});
	}
}
