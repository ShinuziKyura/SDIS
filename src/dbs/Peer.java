package dbs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.NetworkInterface;
import java.net.InetAddress;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.stream.Stream;

import connection.DatagramConnection;
import connection.MulticastConnection;

public class Peer implements PeerInterface {
	/*
		Main
	 */
	private static final String PROTOCOL_VERSION_REGEX =
			"[0-9]\\.[0-9]";
	private static final String ADDRESS_REGEX =
			"(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\." +
			"(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\\.(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])";
	private static final String PORT_REGEX =
			"6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|6[0-4][0-9]{3}|[1-5][0-9]{4}|[1-9][0-9]{1,3}|[0-9]";

	public static void main(String[] args) throws PeerException, IOException {
		/*if (args.length != 8 || !(Pattern.matches(PROTOCOL_VERSION_REGEX, args[0]) &&
								  Pattern.matches(ADDRESS_REGEX, args[2]) && Pattern.matches(PORT_REGEX, args[3]) &&
								  Pattern.matches(ADDRESS_REGEX, args[4]) && Pattern.matches(PORT_REGEX, args[5]) &&
								  Pattern.matches(ADDRESS_REGEX, args[6]) && Pattern.matches(PORT_REGEX, args[7]))) {
			// Print usage and terminate
		}*/

		Peer peer = new Peer("1.0", 1234567890,"DBS_TEST",
							 "225.0.0.0", 8000,
							 "225.0.0.0", 8001,
							 "225.0.0.0", 8002);

		peer.run();
		
	}
	/*
		Internal classes
	 */
	class Version {
		public final int MAJOR_NUMBER;
		public final int MINOR_NUMBER;

		public Version(int major_number, int minor_number) {
			MAJOR_NUMBER = major_number;
			MINOR_NUMBER = minor_number;
		}
	}

	private static final long MAXIMUM_FILE_SIZE = 63999999999L;
	private static final long MAXIMUM_CHUNK_SIZE = 64000L;

	/*
		Member variables
	 */
	public final Version PROTOCOL_VERSION;
	public final String ACCESS_POINT;

	Hashtable<String, String> fileIDs;

	String id;
	Random generator;
	ThreadPoolExecutor executor;
	
	MulticastConnection MCSocket;  // multicast control
	MulticastConnection MDBSocket; // multicast data backup
	MulticastConnection MDRSocket; // multicast data restore

	AtomicBoolean running;

	private PeerChannel MCChannel;
	private PeerProcessor MCProcessor;
	private PeerChannel MDBChannel;
	private PeerProcessor MDBProcessor;
	private PeerChannel MDRChannel;
	private PeerProcessor MDRProcessor;

	private ConcurrentLinkedQueue<byte[]> confirmation_messages;

	/*
		Constructors
	 */
	public Peer(String protocol_version, int id, String access_point,
				String MC_address, int MC_port,
				String MDB_address, int MDB_port,
				String MDR_address, int MDR_port) throws PeerException, IOException {
		PROTOCOL_VERSION = new Version(Character.getNumericValue(protocol_version.charAt(0)),
									   Character.getNumericValue(protocol_version.charAt(2)));
		ACCESS_POINT = access_point;

		System.out.println("\nInitializing peer...");

/*		try {
			byte[] hw_addr = testInterfaces().getHardwareAddress();
			id = Long.toString(ProcessHandle.current().pid()).concat(
					String.format("@%02x%02x%02x%02x%02x%02x",
								  hw_addr[0],
								  hw_addr[1],
								  hw_addr[2],
								  hw_addr[3],
								  hw_addr[4],
								  hw_addr[5]));
		}
		catch (NullPointerException e) {
			throw new PeerException("Could not establish a connection through any available interface" +
									" - Distributed Backup Service unavailable");
		} */

		this.id = Integer.toString(id);

		generator = new Random(ProcessHandle.current().pid());
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		MCSocket = new MulticastConnection(MC_address, MC_port);
		MDBSocket = new MulticastConnection(MDB_address, MDB_port);
		MDRSocket = new MulticastConnection(MDR_address, MDR_port);

		MCChannel = new PeerChannel(this, MCSocket);
		MDBChannel = new PeerChannel(this, MDBSocket);
		MDRChannel = new PeerChannel(this, MDRSocket);

		MCProcessor = new PeerProcessor(this, MCChannel.link());
		MDBProcessor = new PeerProcessor(this, MDBChannel.link());
		MDRProcessor = new PeerProcessor(this, MDRChannel.link());


		// Adjust this
		Registry registry;
		
		try {
			registry = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
		}
		catch(Exception e) {
			registry = LocateRegistry.getRegistry(Registry.REGISTRY_PORT);
		}

		try {
			registry.bind(ACCESS_POINT, UnicastRemoteObject.exportObject(this, 0));
		}
		catch (AlreadyBoundException e) {
			UnicastRemoteObject.unexportObject(this, true);
			throw new PeerException("Access point \"" + ACCESS_POINT + "\" is already in use");
		}

		running = new AtomicBoolean(false);

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
		executor.execute(MCProcessor);
		executor.execute(MDBChannel);
		executor.execute(MDBProcessor);
		executor.execute(MDRChannel);
		executor.execute(MDRProcessor);

		// TODO

		// (Re)move this code after this function is complete
/*		try {
			LocateRegistry.getRegistry(Registry.REGISTRY_PORT).unbind(ACCESS_POINT);
		}
		catch (NotBoundException e) {
			// That's weird, shouldn't happen
		}

		UnicastRemoteObject.unexportObject(this, false);

		running.set(false);

		MCSocket.close();
		MDBSocket.close();
		MDRSocket.close();

		executor.shutdown();*/
	}

	public void backup(String pathname, int replication_degree) throws IOException {
		if (replication_degree < 1 || replication_degree > 9) {
			// No can do
			return;
		}

		File file = new File(pathname);
		if (!file.exists()) {
			// Say something
			return;
		}

		long size = 0;
		try {
			size = Files.size(file.toPath());
		}
		catch (IOException e) {
			// No idea why this can throw
		}
		if (size > MAXIMUM_FILE_SIZE || size == 0) {
			// No can do
			return;
		}

		String file_id = PeerProtocol.hash(pathname +
										   Files.getLastModifiedTime(file.toPath()).toString().split("T")[0]);

		long chunk_count = 0;
		byte[] chunk_body = new byte[(int) MAXIMUM_CHUNK_SIZE];

		//try-with-resources to ensure closing stream
		try (FileInputStream filestream = new FileInputStream(file);
			 BufferedInputStream buffer = new BufferedInputStream(filestream)) {

			int chunk_body_length = 0;
			while ((chunk_body_length = buffer.read(chunk_body)) > 0) {
				byte[] chunk_header = ("PUTCHUNK " + PROTOCOL_VERSION.MAJOR_NUMBER + "." + PROTOCOL_VERSION.MINOR_NUMBER + " " + id + " " + file_id + " " + chunk_count++ + " " + replication_degree + " \r\n\r\n").getBytes();
				byte[] chunk = merge(chunk_header, chunk_body);
			/*	while () {
					MDBSocket.send(chunk);
				}*/
			}
			if (chunk_body_length == MAXIMUM_CHUNK_SIZE) {

			}
		}
		catch (FileNotFoundException e) {
			// Shouldn't happen
		}

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

	/*
		Internal functions
	 */
	// Code adapted from here: https://stackoverflow.com/a/8462548
	public static NetworkInterface testInterfaces() throws IOException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		for (NetworkInterface netInt : Collections.list(interfaces)) {
			// we don't care about loopback addresses
			// or interfaces that aren't up and running
			if (netInt.isLoopback() || !netInt.isUp()) {
				continue;
			}
			// iterate over the addresses associated with the interface
			Enumeration<InetAddress> addresses = netInt.getInetAddresses();
			for (InetAddress InetAddr : Collections.list(addresses)) {
				// we look only for ipv4 addresses
				// and use a timeout big enough for our needs
				if (InetAddr instanceof Inet6Address || !InetAddr.isReachable(1000)) {
					continue;
				}
				// java 7's try-with-resources statement, so that
				// we close the socket immediately after use
				try (SocketChannel socket = SocketChannel.open()) {
					// bind the socket to our local interface
					socket.bind(new InetSocketAddress(InetAddr, 0));

					// try to connect to *somewhere*
					socket.connect(new InetSocketAddress("example.com", 80));
				}
				catch (IOException e) {
					continue;
				}

				// stops at the first *working* solution
				return netInt;
			}
		}
		return null;
	}

	// Because JAVA is SOOOOO high level
	public static byte[] merge(byte[] first, byte[] second) {
		byte[] result = new byte[first.length + second.length];
		for (int i = 0; i < result.length; ++i) {
			result[i] = (i < first.length ? first[i] : second[i - first.length]);
		}
		return result;
	}
}
