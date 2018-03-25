package dbs;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.io.IOException;

public class DatagramConnection {
	protected DatagramPacket packet;
	protected DatagramSocket socket;
	protected byte[] buffer;
	
//	protected static final String EOM = "@EOM"; // Needs adjustment
	protected static final int BUFFER_SIZE = 65536;

	public DatagramConnection(int port) throws IOException {
		this.socket = new DatagramSocket(port);
		this.buffer = new byte[BUFFER_SIZE];
	}
	
	protected DatagramConnection() {
		this.buffer = new byte[BUFFER_SIZE];
	}
	
/*	public void send(String message, String address, int port) throws IOException {
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
	
/*	public String receive() throws IOException {
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
	
	public byte[] receive() throws IOException {
		this.buffer = new byte[BUFFER_SIZE];
		
		this.packet = new DatagramPacket(this.buffer, BUFFER_SIZE);
			
		this.socket.receive(this.packet);
		
		return this.buffer;
	}
	
	public void close() throws IOException {
		this.socket.close();
	}
}
