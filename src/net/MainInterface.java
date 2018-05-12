package net;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Enumeration;

public class MainInterface {
	// Code adapted from here: https://stackoverflow.com/a/8462548
	public static NetworkInterface find() throws IOException {
		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		for (NetworkInterface netInt : Collections.list(interfaces)) {
			// we don't care about loopback addresses
			// or interfaces that aren't up and running
			if (netInt.isLoopback() || !netInt.isUp()) {
				continue;
			}
			// iterate over the addresses associated with the interface
			Enumeration<InetAddress> addresses = netInt.getInetAddresses();
			for (InetAddress inetAddr : Collections.list(addresses)) {
				// we look only for ipv4 addresses
				// and use a timeout big enough for our needs
				if (inetAddr instanceof Inet6Address || !inetAddr.isReachable(1000)) {
					continue;
				}
				// java 7's try-with-resources statement, so that
				// we close the socket immediately after use
				try (SocketChannel socket = SocketChannel.open()) {
					// bind the socket to our local interface
					socket.bind(new InetSocketAddress(inetAddr, 0));

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
