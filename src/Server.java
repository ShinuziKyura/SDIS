import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
import java.io.IOException;

public class Server {
	private DatagramConnection socket;
	private Hashtable<String, String> info;
	private Timer timer;
	
	public static String READER_ADDRESS = "89.152.47.157";
	
	private static final String NULL_ADDRESS = "0.0.0.0";
	private static final int NULL_PORT = 0;

	/*private class DatagramService extends TimerTask {
		private MulticastConnection socket;
		private String message;
		
		public static final int period = 1000;
		
		public DatagramService(int socket, String address, int port) throws IOException {
			this.service_socket = new DatagramConnection(NULL_PORT, address, port);
			this.message = "multicast "+address+":"+port+" - "+ADDRESS+":"+socket;
		}
		
		public void run() {
			try {
				this.service_socket.send(message);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}*/

	public Server(int socket, String address, int port) throws IOException {
		this.socket = new DatagramConnection(socket);
		this.info = new Hashtable<String, String>();
		//this.timer = new Timer();
		//this.timer.schedule(new DatagramService(socket, address, port), 0, DatagramService.period);
	}

	public void processRequests() throws IOException {
		String[] request = null;
		do {
			request = socket.receive().split(" ");
		} while (this.process(request));
	}


	private boolean process(String[] request) throws IOException {
		switch (request[0])
		{
		case "REGISTER":
			if(!info.containsKey(request[1])){
				info.put(request[1],request[2]);
				socket.send(info.size()+"", "localhost", 25565);
				System.out.println("Registered vehicle!");
			}
			else{
				socket.send(-1+"", "localhost", 25565);
				System.out.println("Unable to register vehicle...");
			}
			break;
		case "LOOKUP":
			if(info.containsKey(request[1])){
				socket.send(info.get(request[1]), "localhost", 25565);
				System.out.println("Vehicle found!");
			}
			else{
				socket.send("NOT_FOUND", "localhost", 25565);
				System.out.println("Vehicle not found!");
			}
			break;
		case "CLOSE":
			System.out.println(request[0]);
			socket.close();
			return false;
		}
		return true;
	}
	
	public static void main(String[] args) throws IOException {
		Server s = new Server(25566, NULL_ADDRESS, NULL_PORT);

		s.processRequests();
	}
}
