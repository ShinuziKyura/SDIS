package dbs;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import dbs.util.PeerUtility;
import net.MulticastChannel;
import util.concurrent.LinkedTemporaryQueue;

import static dbs.util.PeerUtility.*;

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

		Peer peer = Peer.initialize(args[0], Integer.valueOf(args[1]), args[2],
		                            args[3], Integer.valueOf(args[4]),
		                            args[5], Integer.valueOf(args[6]),
		                            args[7], Integer.valueOf(args[8]));
	/*	Peer peer = Peer.initialize("1.0", 1,"DBS_TEST",
							 "225.0.0.0", 8000,
							 "225.0.0.0", 8001,
							 "225.0.0.0", 8002); */

		peer.run();
		peer.terminate();
	}

	private static Peer instance = null;

	public static Peer initialize(String protocol_version, int id, String access_point,
	                              String MC_address, int MC_port,
	                              String MDB_address, int MDB_port,
	                              String MDR_address, int MDR_port) throws PeerException, IOException {
		if (instance != null) {
			instance = new Peer(protocol_version, id, access_point,
			                    MC_address, MC_port, MDB_address, MDB_port, MDR_address, MDR_port);
		}
		return instance;
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
	final Version PROTOCOL_VERSION;
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

	Hashtable<String, LinkedTemporaryQueue<byte[]>> DBReplies; // Name subject to change

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
	private Peer(String protocol_version, int id, String access_point,
				String MC_address, int MC_port,
				String MDB_address, int MDB_port,
				String MDR_address, int MDR_port) throws PeerException, IOException {
		System.out.println("\nInitializing peer...");

		/* Single-comment this line to switch to Cool-Mode
		NetworkInterface net_int = PeerUtility.testInterfaces();
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

		PROTOCOL_VERSION = new PeerUtility.Version(protocol_version);
		ACCESS_POINT = access_point;

		processes = new AtomicInteger(0);
		running = new AtomicBoolean(false);

		generator = new Random(ProcessHandle.current().pid());
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		stored_files = new Hashtable<>();
		stored_chunks = new Hashtable<>();

		// load data from files

		DBReplies = new Hashtable<>();

		MCSocket = new MulticastChannel(MC_address, MC_port);
		MDBSocket = new MulticastChannel(MDB_address, MDB_port);
		MDRSocket = new MulticastChannel(MDR_address, MDR_port);

		MCChannel = new PeerChannel(this, MCSocket);
		MDBChannel = new PeerChannel(this, MDBSocket);
		MDRChannel = new PeerChannel(this, MDRSocket);

		MCQueue = new PeerQueue(this, MCChannel.queue());
		MDBQueue = new PeerQueue(this, MDBChannel.queue());
		MDRQueue = new PeerQueue(this, MDRChannel.queue());

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
		                   "\n\tAccess point: " + ACCESS_POINT +
		                   "\n\nRunning: false");
	}

	/*
		Member functions
	 */
	public void run() {
		running.set(true);

		executor.execute(MCChannel);
		executor.execute(MDBChannel);
		executor.execute(MDRChannel);

		executor.execute(MCQueue);
		executor.execute(MDBQueue);
		executor.execute(MDRQueue);

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
	}

	public int backup(String filename, String fileID, byte[] file, int replication_degree) {
		if (processes.getAndIncrement() < 0) {
			System.err.println("\nERROR! Peer process terminating...");
			processes.decrementAndGet();
			return 1;
		}

		if (file.length == 0 || file.length > MAXIMUM_FILE_SIZE) {
			System.err.println("\nERROR! File must be greater than 0 bytes and less than 64 gigabytes" +
			                   "\nBACKUP protocol terminating...");
			processes.decrementAndGet();
			return 11;
		}
		if (replication_degree < 1 || replication_degree > 9) {
			System.err.println("\nERROR! Replication degree must be greater than 0 and less than 10" +
			                   "\nBACKUP protocol terminating...");
			processes.decrementAndGet();
			return 12;
		}

		String old_fileID = stored_files.put(filename, fileID);
		if (old_fileID != null) {
			executor.execute(new PeerProtocol(this, PeerUtility.generateProtocolHeader(
					MessageType.DELETE, PROTOCOL_VERSION, ID, old_fileID,null, null)));
		}

		DBReplies.put(fileID, new LinkedTemporaryQueue<>());
		LinkedTemporaryQueue<byte[]> replies = DBReplies.get(fileID);

		int chunk_count = 0;
		int chunk_amount = file.length / MAXIMUM_CHUNK_SIZE + 1;
		do {
			int chunk_size = (chunk_count + 1) * MAXIMUM_CHUNK_SIZE < file.length ? (chunk_count + 1) * MAXIMUM_CHUNK_SIZE : file.length;
			byte[] chunk_header = PeerUtility.generateProtocolHeader(MessageType.PUTCHUNK, PROTOCOL_VERSION, ID, fileID, chunk_count, replication_degree);
			byte[] chunk_body = Arrays.copyOfRange(file, chunk_count * MAXIMUM_CHUNK_SIZE, chunk_size);
			byte[] chunk = PeerUtility.mergeArrays(chunk_header, chunk_body);

			int stored = 0;
			int requests = 0;
			while (stored < replication_degree && requests < 5) {
				try {
					MDBSocket.send(chunk);
				}
				catch (IOException e) {
					// Shouldn't happen
				}

				replies.activate((1 << requests++) * 1000);
				while (replies.poll() != null) {
					++stored;
				}
			}
			if (stored == 0) {
				System.err.println("\nERROR! Chunk " + chunk_count + " could not be stored" +
				                   "\nBACKUP protocol terminating...");
				executor.execute(new PeerProtocol(this, PeerUtility.generateProtocolHeader(
						MessageType.DELETE, PROTOCOL_VERSION, ID, stored_files.remove(filename),null, null)));
				processes.decrementAndGet();
				return 13;
			}
			if (stored < replication_degree) {
				System.out.println("\nWARNING! Replication degree could not be met:" +
				                   "\n\tRequested - " + replication_degree +
				                   "\n\tActual - " + stored);
			}
		} while (++chunk_count < chunk_amount);

		// Maybe wake up Peer thread to save hashtable to file

		processes.decrementAndGet();

		return 0;
	}

	public int restore(String pathname) {
		// TODO
		return 0;
	}

	public int delete(String pathname) {
		// TODO
		return 0;
	}

	public int reclaim(int disk_space) {
		// TODO
		return 0;
	}

	public String state() {
		// TODO
		return null;
	}

	public int stop() {
		while (!processes.weakCompareAndSetPlain(0, Integer.MIN_VALUE));

		running.set(false);

		MCChannel.stop();
		MDBChannel.stop();
		MDRChannel.stop();

		MCQueue.stop();
		MDBQueue.stop();
		MDRQueue.stop();

		// This will be removed
		synchronized (running) {
			running.notifyAll();
		}

		return 0;
	}

	public int stop(long duration) {
		AtomicBoolean timeout = new AtomicBoolean(false);
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				timeout.set(true);
			}
		}, duration);

		while (!processes.weakCompareAndSetPlain(0, Integer.MIN_VALUE)) {
			if (timeout.get()) {
				return 1;
			}
		}

		running.set(false);

		MCChannel.stop();
		MDBChannel.stop();
		MDRChannel.stop();

		MCQueue.stop();
		MDBQueue.stop();
		MDRQueue.stop();

		// This will be removed
		synchronized (running) {
			running.notifyAll();
		}

		return 0;
	}

	public void terminate() {
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

		// save data to files

		executor.shutdown();

		System.out.println("\nPeer terminated");

		instance = null;
	}
}
