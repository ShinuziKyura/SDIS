import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.io.IOException;

public class DatagramConnection {
	protected DatagramPacket packet;
	protected DatagramSocket socket;
	protected byte[] buffer;
	
	protected static final String EOM = "@EOM";

	public DatagramConnection(int port) throws IOException {
		this.socket = new DatagramSocket(port);
		this.buffer = new byte[1024];
	}
	
	public void send(String message, String address, int port) throws IOException {
		int packet_amount = (message.length() - 1) / buffer.length + 1;
		
		String message_part = "";
		
		for(int idx = 0; idx < packet_amount; ++idx) {
			message_part = message.substring(idx * buffer.length, Math.min((idx + 1) * buffer.length, message.length()));

			packet = new DatagramPacket(message_part.getBytes(), message_part.length(), InetAddress.getByName(address), port);
			
			socket.send(packet);
		}
			
		packet = new DatagramPacket(EOM.getBytes(), EOM.length(), InetAddress.getByName(address), port);
		
		socket.send(packet);
	}
	
	public String receive() throws IOException {
		packet = new DatagramPacket(buffer, buffer.length);
		
		String message = "";
		String message_part = "";
		
		do {
			message += message_part;
			
			socket.receive(packet);

			message_part = new String(packet.getData(), 0, packet.getLength());
		} while (message_part.compareTo(EOM) != 0);
		
		return message;
	}
	
	public void close() throws IOException {
		this.socket.close();
	}
}
