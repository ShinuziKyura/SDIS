import java.io.IOException;

import dbs.DatagramConnection;

public class Client extends DatagramConnection{
	public static void main(String[] args) throws IOException {
		Client c = new Client(25565);
	
		c.register("11-A2-34 jose", "localhost", 25566);
		System.out.println(c.receive());
		c.register("13-B2-36 maria", "localhost", 25566);
		System.out.println(c.receive());
		c.register("13-B2-36 josefina", "localhost", 25566);
		System.out.println(c.receive());
		c.lookup("13-B2-36", "localhost", 25566);
		System.out.println(c.receive());
		c.lookup("13-B2-37", "localhost", 25566);
		System.out.println(c.receive());
		c.close("localhost", 25566);
	}
	
	public Client(int port) throws IOException{
		super(port);
	}
	
	public void register(String message, String ip, int port) throws IOException
	{
		this.send("REGISTER " + message, ip, port);
	}
	
	public void lookup(String message, String ip, int port) throws IOException
	{
		this.send("LOOKUP " + message, ip, port);
	}
	
	public void close(String ip, int port) throws IOException
	{
		this.send("CLOSE", ip, port);
	}
}
