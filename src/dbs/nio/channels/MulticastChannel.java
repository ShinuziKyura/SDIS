package dbs.nio.channels;

import java.net.InetAddress;
import java.net.MulticastSocket;
import java.io.IOException;

public class MulticastChannel extends DatagramChannel {
	private String address;
	private int port;
	
	public static int TTL = 5;

	public MulticastChannel(String address, int port) throws IOException {
		this.buffer = new byte[BUFFER_SIZE];
		this.socket = new MulticastSocket(port);

		this.address = address;
		this.port = port;
		
		if (TTL != 0) {
			((MulticastSocket) this.socket).setTimeToLive(TTL);
		}
		
		((MulticastSocket) this.socket).joinGroup(InetAddress.getByName(this.address));
	}
	
/*	public void send(String message) throws IOException {
		super.send(message, this.address, this.port);
	} */

	public void send(byte[] message) throws IOException {
		super.send(message, this.address, this.port);
	}

	public void close() throws IOException {
		((MulticastSocket) this.socket).leaveGroup(InetAddress.getByName(this.address));

		super.close();
	}
}
