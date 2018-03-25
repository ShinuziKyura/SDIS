package dbs;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class Peer {
	private int id;
	private MulticastConnection mcSocket;  // multicast control
	private MulticastConnection mdbSocket; // multicast data backup
	private MulticastConnection mdrSocket; // multicast data restore
	private ScheduledThreadPoolExecutor threadpool;
	private PeerListener mcListener;
	private PeerListener mdbListener;
	private PeerListener mdrListener;
	
	public static String DBS_TEST = "dbstest";
	
	public Peer(char[] protocol_version, int server_id, String access_point, 
				String mc_addr, short mc_port, 
				String mdb_addr, short mdb_port, 
				String mdr_addr, short mdr_port) throws IOException
	{
		id = server_id;
		mcSocket = new MulticastConnection(mc_addr, mc_port);
		mdbSocket = new MulticastConnection(mdb_addr, mdb_port);
		mdrSocket = new MulticastConnection(mdr_addr, mdr_port);
		threadpool = new ScheduledThreadPoolExecutor(3);
		
	}
	
	public boolean backup(String path, int rep_deg)
	{
		
		return true;
	}
}
