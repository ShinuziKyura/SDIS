package nio.channels;

import java.net.InetAddress;
import java.util.Arrays;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.io.IOException;

public class DatagramChannel {
	protected byte[] buffer;

	protected DatagramPacket packet;
	protected DatagramSocket socket;

//	protected static final String EOM = "@EOM"; // Actually we require some form of byte stuffing
	public static final int BUFFER_SIZE = 65536; // Public for now

	public static class DatagramPackage {
		public final InetAddress address;
		public final byte[] message;

		public DatagramPackage(InetAddress address, byte[] message) {
			this.address = address;
			this.message = message;
		}
	}

	protected DatagramChannel() {
	}

	public DatagramChannel(int port) throws IOException {
		this.buffer = new byte[BUFFER_SIZE];
		this.socket = new DatagramSocket(port);
	}
	
/*	public void send(String message, String address, int port) throws IOException { // Probably better to receive byte[]
		int packet_amount = (message.length() - 1) / BUFFER_SIZE + 1;
		
		String message_part = "";
		
		for(int idx = 0; idx < packet_amount; ++idx) {
			message_part = message.substring(idx * BUFFER_SIZE, Math.min((idx + 1) * BUFFER_SIZE, message.length()));

			this.packet = new DatagramPacket(message_part.getBytes(), message_part.length(), InetAddress.getByName(address), port);
			
			this.socket.send(this.packet);
		}
			
		this.packet = new DatagramPacket(EOM.getBytes(), EOM.length(), InetAddress.getByName(address), port);
		
		this.socket.send(this.packet);
	} */
	
	public void send(byte[] message, String address, int port) throws IOException {
		this.packet = new DatagramPacket(message, message.length, InetAddress.getByName(address), port);
			
		this.socket.send(this.packet);
	}
	
/*	public String receive() throws IOException { // Probably better to return byte[]
		this.buffer = new byte[BUFFER_SIZE];

		this.packet = new DatagramPacket(this.buffer, BUFFER_SIZE);
		
		String message = "";
		String message_part = "";
		
		do {
			message += message_part;
			
			this.socket.receive(this.packet);

			message_part = new String(this.packet.getData(), 0, this.packet.getLength());
		} while (message_part.compareTo(EOM) != 0);
		
		return message;
	}*/

	/*public byte[] receive() throws IOException {
		this.buffer = new byte[BUFFER_SIZE];

		this.packet = new DatagramPacket(this.buffer, BUFFER_SIZE);

		this.socket.receive(this.packet);
		
		return Arrays.copyOf(this.packet.getData(), this.packet.getLength());
	}*/

	public DatagramPackage receive() throws IOException {
		this.buffer = new byte[BUFFER_SIZE];

		this.packet = new DatagramPacket(this.buffer, BUFFER_SIZE);

		this.socket.receive(this.packet);

		return new DatagramPackage(this.packet.getAddress(), Arrays.copyOf(this.packet.getData(), this.packet.getLength()));
	}

	public void close() throws IOException {
		this.socket.close();
	}
}
