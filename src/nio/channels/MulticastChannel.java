package nio.channels;

import java.net.InetAddress;
import java.net.MulticastSocket;
import java.io.IOException;

public class MulticastChannel extends DatagramChannel {
	public final String address;
	public final int port;
	
	public static int TTL = 5;

	public MulticastChannel(String address, int port) throws IOException, IllegalArgumentException {
		this.buffer = new byte[BUFFER_SIZE];
		this.socket = new MulticastSocket(port);

		String[] octets = address.split("\\.");
		long decimal = (Long.valueOf(octets[0]) << 24) + (Long.valueOf(octets[1]) << 16) + (Long.valueOf(octets[2]) << 8) + Long.valueOf(octets[3]);

		if (decimal < 3758096384L || decimal > 4026531839L) {
			throw new IllegalArgumentException("Address must be between 224.0.0.0 and 239.255.255.255");
		}
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("Port must be a number between 1 and 65535");
		}

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
