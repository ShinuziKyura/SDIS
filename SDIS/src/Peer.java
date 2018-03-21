import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class Peer {
	private int id;
	private MulticastConnection mc_socket;
	private MulticastConnection mdb_socket;
	private MulticastConnection mdr_socket;
	
	public static String DBS_TEST = "dbstest";
	
	public Peer(char[3] protocol_version, int server_id, String access_point, 
				String mc_addr, short mc_port, 
				String mdb_addr, short mdb_port, 
				String mdr_addr, short mdr_port)
	{
		id = server_id;
		mc_socket = new MulticastConnection(mc_addr, mc_port);
		mdb_socket = new MulticastConnection(mdb_addr, mdb_port);
		mdr_socket = new MulticastConnection(mdr_addr, mdr_port);
	}
}
