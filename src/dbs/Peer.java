package dbs;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Peer {
	private Random generator;
	private ThreadPoolExecutor executor;

	private int id;
	
	private MulticastConnection mcSocket;  // multicast control
	private MulticastConnection mdbSocket; // multicast data backup
	private MulticastConnection mdrSocket; // multicast data restore
	
	public static String DBS_TEST = "dbstest";
	
	public Peer(char[] protocol_version, int server_id, String access_point, 
				String mc_addr, short mc_port, 
				String mdb_addr, short mdb_port, 
				String mdr_addr, short mdr_port) throws IOException
	{
		generator = new Random(ProcessHandle.current().pid());
		executor = new ThreadPoolExecutor(8, 15, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
		
		id = Math.abs(generator.nextInt() + 1);
		mcSocket = new MulticastConnection(mc_addr, mc_port);
		mdbSocket = new MulticastConnection(mdb_addr, mdb_port);
		mdrSocket = new MulticastConnection(mdr_addr, mdr_port);
		
		executor.execute(new PeerChannel(this, mcSocket));
		executor.execute(new PeerChannel(this, mdbSocket));
		executor.execute(new PeerChannel(this, mdrSocket));
		
		// Goto client interface
	}
	
	public boolean backup(String path, int rep_deg)
	{
		
		return true;
	}
}
