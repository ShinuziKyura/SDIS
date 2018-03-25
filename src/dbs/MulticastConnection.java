package dbs;
import java.net.MulticastSocket;
import java.net.InetAddress;
import java.io.IOException;

public class MulticastConnection extends DatagramConnection {
	private String address;
	private int port;
	
	public static final boolean FEUP_NET = true;

	public MulticastConnection(String address, int port) throws IOException {
		super();
		
		this.socket = new MulticastSocket(port);
		this.address = address;
		this.port = port;
		
		if (FEUP_NET) {
			((MulticastSocket) this.socket).setTimeToLive(1);
		}
		
		((MulticastSocket) this.socket).joinGroup(InetAddress.getByName(this.address));
	}
	
	public void send(String message) throws IOException {
		super.send(message, this.address, this.port);
	}
	
	public String receive() throws IOException {
		return super.receive();
	}
	
	public void close() throws IOException {
		((MulticastSocket) this.socket).leaveGroup(InetAddress.getByName(this.address));
		
		this.socket.close();
	}
}
