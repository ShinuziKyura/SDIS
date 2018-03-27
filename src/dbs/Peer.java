package dbs;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.Collections;
import java.util.Enumeration;
import java.net.NetworkInterface;
import java.net.InetAddress;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

public class Peer {
	String id;
	Random generator;
	ThreadPoolExecutor executor;
	
	MulticastConnection MCSocket;  // multicast control
	MulticastConnection MDBSocket; // multicast data backup
	MulticastConnection MDRSocket; // multicast data restore

	private PeerChannel MCChannel;
	private PeerChannel MDBChannel;
	private PeerChannel MDRChannel;
	
	public static String DBS_TEST = "dbstest";
	
	public Peer(String protocol_version, String access_point,
				String mc_addr, int mc_port,
				String mdb_addr, int mdb_port,
				String mdr_addr, int mdr_port) throws IOException {
		System.out.println("Initializing peer...");

		// place try catch, terminate if it throws
		byte[] hw_addr = TestInterface().getHardwareAddress();
		id = Long.toString(ProcessHandle.current().pid()).concat(
				String.format("@%02x%02x%02x%02x%02x%02x", hw_addr[0], hw_addr[1], hw_addr[2], hw_addr[3], hw_addr[4], hw_addr[5]));
		System.out.println("Peer initialized with id: " + id);

		generator = new Random(ProcessHandle.current().pid());
		executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

		MCSocket = new MulticastConnection(mc_addr, mc_port);
		MDBSocket = new MulticastConnection(mdb_addr, mdb_port);
		MDRSocket = new MulticastConnection(mdr_addr, mdr_port);

		executor.execute(MCChannel = new PeerChannel(this, MCSocket));
		executor.execute(MDBChannel = new PeerChannel(this, MDBSocket));
		executor.execute(MDRChannel = new PeerChannel(this, MDRSocket));

		// Goto client interface

		try {
			TimeUnit.MILLISECONDS.sleep(1000);
		}
		catch (InterruptedException e) {

		}

		MCChannel.stop();
		MDBChannel.stop();
		MDRChannel.stop();

		MCSocket.close();
		MDBSocket.close();
		MDRSocket.close();

		executor.shutdown();
	}

	// Code adapted from here: https://stackoverflow.com/a/8462548
	public static NetworkInterface TestInterface() throws IOException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		for (NetworkInterface netInt : Collections.list(interfaces)) {
			// we don't care about loopback addresses
			// or interfaces that aren't up and running
			if (netInt.isLoopback() || !netInt.isUp())
				continue;

			// iterate over the addresses associated with the interface
			Enumeration<InetAddress> addresses = netInt.getInetAddresses();
			for (InetAddress InetAddr : Collections.list(addresses)) {
				// we look only for ipv4 addresses
				// and use a timeout big enough for our needs
				if (InetAddr instanceof Inet6Address || !InetAddr.isReachable(1000))
					continue;

				// java 7's try-with-resources statement, so that
				// we close the socket immediately after use
				try (SocketChannel socket = SocketChannel.open()) {
					// bind the socket to our local interface
					socket.bind(new InetSocketAddress(InetAddr, 0));

					// try to connect to *somewhere*
					socket.connect(new InetSocketAddress("example.com", 80));
				} catch (IOException ex) {
					continue;
				}

				// stops at the first *working* solution
				return netInt;
			}
		}
		return null;
	}
}
