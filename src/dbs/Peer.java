package dbs;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Collections;
import java.util.Enumeration;
import java.net.NetworkInterface;
import java.net.InetAddress;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

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

	/*
		Member variables
	 */
	public final Version PROTOCOL_VERSION;
	public final String ACCESS_POINT;

	String id; // id needs to be an integer
	Random generator;
	ThreadPoolExecutor executor;
	
	MulticastConnection MCSocket;  // multicast control
	MulticastConnection MDBSocket; // multicast data backup
	MulticastConnection MDRSocket; // multicast data restore

	private PeerChannel MCChannel;
	private PeerChannel MDBChannel;
	private PeerChannel MDRChannel;

	private AtomicBoolean running;

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
		executor.execute(MCChannel);
		executor.execute(MDBChannel);
		executor.execute(MDRChannel);

		running.setRelease(true);

		// TODO

		// (Re)move this code after this function is complete
/*		try {
			LocateRegistry.getRegistry(Registry.REGISTRY_PORT).unbind(ACCESS_POINT);
		}
		catch (NotBoundException e) {
			// That's weird
		}

		UnicastRemoteObject.unexportObject(this, false);

		MCChannel.stop();
		MDBChannel.stop();
		MDRChannel.stop();

		MCSocket.close();
		MDBSocket.close();
		MDRSocket.close();

		executor.shutdown();*/
	}

	public void backup(String pathname, int replication_degree) {
		// TODO
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
}
