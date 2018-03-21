package dbs;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class Peer {
	private int id;
	private MulticastConnection mc_socket; //multicast control
	private MulticastConnection mdb_socket; // multicast data backup
	private MulticastConnection mdr_socket; // multicast data restore
	
	public static String DBS_TEST = "dbstest";
	
	public Peer(char[] protocol_version, int server_id, String access_point, 
				String mc_addr, short mc_port, 
				String mdb_addr, short mdb_port, 
				String mdr_addr, short mdr_port) throws IOException
	{
		id = server_id;
		mc_socket = new MulticastConnection(mc_addr, mc_port);
		mdb_socket = new MulticastConnection(mdb_addr, mdb_port);
		mdr_socket = new MulticastConnection(mdr_addr, mdr_port);
	}
}
