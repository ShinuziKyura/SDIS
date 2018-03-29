package dbs;

import java.io.IOException;
import java.net.NetworkInterface;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Hashtable;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
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
		/* Single-comment this line to activate the check
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
	private static final long MAXIMUM_CHUNK_SIZE = 64000L;

	/*
		Member constants
	 */
	final PeerUtility.Version PROTOCOL_VERSION;
	final String ACCESS_POINT;

	/*
		Member variables
	 */
	Hashtable<String, String> file_names_to_IDs; // Read from file

	String id;
	Random generator;
	ThreadPoolExecutor executor;
	
	MulticastChannel MCSocket;  // multicast control
	MulticastChannel MDBSocket; // multicast data backup
	MulticastChannel MDRSocket; // multicast data restore

	Hashtable<String, LinkedTemporaryQueue<byte[]>> DBReplies; // Name subject to change

	AtomicBoolean running;

	private PeerChannel MCChannel;
	private PeerChannel MDBChannel;
	private PeerChannel MDRChannel;

	private PeerProcessor MCProcessor;
	private PeerProcessor MDBProcessor;
	private PeerProcessor MDRProcessor;

	/*
		Constructor
	 */
	public Peer(String protocol_version, int id, String access_point,
				String MC_address, int MC_port,
				String MDB_address, int MDB_port,
				String MDR_address, int MDR_port) throws PeerException, IOException {
		PROTOCOL_VERSION = new PeerUtility.Version(protocol_version);
		ACCESS_POINT = access_point;

		System.out.println("\nInitializing peer...");

		/* <- Single-comment this line to switch to Cool-Mode
		NetworkInterface net_int = PeerUtility.testInterfaces();
		if (net_int != null) {
			byte[] hw_addr = net_int.getHardwareAddress();
			this.id = String.format("%02x%02x%02x%02x%02x%02x@",
									hw_addr[0], hw_addr[1], hw_addr[2],
									hw_addr[3], hw_addr[4], hw_addr[5])
							.concat(Long.toString(ProcessHandle.current().pid()));
		}
		else {
			throw new PeerException("Could not establish a net through any available interface" +
									" - Distributed Backup Service unavailable");
		}
		/*/
		this.id = Integer.toString(id);
		//*/

		generator = new Random(ProcessHandle.current().pid());
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		MCSocket = new MulticastChannel(MC_address, MC_port);
		MDBSocket = new MulticastChannel(MDB_address, MDB_port);
		MDRSocket = new MulticastChannel(MDR_address, MDR_port);

		MCChannel = new PeerChannel(this, MCSocket);
		MDBChannel = new PeerChannel(this, MDBSocket);
		MDRChannel = new PeerChannel(this, MDRSocket);

		MCProcessor = new PeerProcessor(this, MCChannel.bind());
		MDBProcessor = new PeerProcessor(this, MDBChannel.bind());
		MDRProcessor = new PeerProcessor(this, MDRChannel.bind());

		DBReplies = new Hashtable<>();

		running = new AtomicBoolean(false);

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

		System.out.println("\nPeer initialized with id: " + id +
						   "\n\tProtocol version: " + protocol_version +
						   "\n\tAccess point: " + access_point);
	}

	/*
		Member functions
	 */
	public void run() throws IOException {
		running.set(true);

		executor.execute(MCChannel);
		executor.execute(MDBChannel);
		executor.execute(MDRChannel);

		executor.execute(MCProcessor);
		executor.execute(MDBProcessor);
		executor.execute(MDRProcessor);

		synchronized (running) {
			while (running.get()) {
				try {
					running.wait();
				} catch (InterruptedException e) {
					// Probably time to terminate
				}
			}
		}

		terminate(); // For now
	}

	public void backup(String filename, String fileID, byte[] file, int replication_degree) {
		System.out.println(filename);
		System.out.println(fileID);
		System.out.println(replication_degree);
		if (file.length == 0 || file.length > MAXIMUM_FILE_SIZE) {
			System.err.println("File must be greater than 0 bytes and less than 64 gigabytes");
			return;
		}
		if (replication_degree < 1 || replication_degree > 9) {
			System.err.println("Replication degree must be greater than 0 and less than 10");
			return;
		}

		/*
//		DBReplies.put(file_id, new LinkedTemporaryQueue<>());

		long chunk_count = 0;
		byte[] chunk_body = new byte[(int) MAXIMUM_CHUNK_SIZE];

		//try-with-resources to ensure closing stream
		try {
			int chunk_body_length = 0;
			while ((chunk_body_length = 0/*buffer.read(chunk_body)) > 0) {
				byte[] chunk_header = ("PUTCHUNK " + PROTOCOL_VERSION.MAJOR_NUMBER + "." + PROTOCOL_VERSION.MINOR_NUMBER + " " + id + " " + fileID + " " + chunk_count++ + " " + replication_degree + " \r\n\r\n").getBytes();
				byte[] chunk = PeerUtility.mergeArrays(chunk_header, chunk_body);

				int stored_chunks = 0;
				int duration = 1;
				while (stored_chunks < replication_degree && duration <= 16) { // dont ask
					MDBSocket.send(chunk);
            //        DBReplies.get(file_id).activate(duration * 1000);
                    duration += duration;

					while (DBReplies.get(fileID).take() != null) {
						++stored_chunks;
					}
				}
			}
			if (chunk_body_length == MAXIMUM_CHUNK_SIZE) {

			}
		}
		catch (FileNotFoundException e) {
			// Shouldn't happen
		}*/

		/*
		//try-with-resources to ensure closing stream
		try (FileInputStream fis = new FileInputStream(file);
			BufferedInputStream bis = new BufferedInputStream(fis)) {

			int bytesAmount = 0;
			while ((bytesAmount = bis.read(chunk_buffer)) > 0) {
				//write each chunk of data into separate file with different number in name
				String chunkname = String.format("%s.%06d", filename, chunk_count++);

				File newFile = new File(f.getParent(), filePartName);
				try (FileOutputStream out = new FileOutputStream(newFile)) {
					out.write(buffer, 0, bytesAmount);
				}
			}
		}
		catch (FileNotFoundException e) {
			// Shouldn't happen
		}
		 */

		/*String old_file_id = fileIDs.get(pathname);*/
		/*if (old_file_id != null) {
			// Execute new PeerProtocol to delete file
		}*/
	}

	public void restore(String pathname) {
		// TODO
	}

	public void delete(String pathname) {
		// TODO
	}

	public void reclaim(int disk_space) {
		// TODO
	}

	public String state() {
		// TODO
		return null;
	}

	public void stop() {
		running.set(false);

		MCChannel.stop();
		MDBChannel.stop();
		MDRChannel.stop();

		MCProcessor.stop();
		MDBProcessor.stop();
		MDRProcessor.stop();

		synchronized (running) {
			running.notifyAll();
		}
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

		executor.shutdown();

		System.out.println("\nPeer terminated");
	}
}
