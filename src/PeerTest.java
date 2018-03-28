import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;

import dbs.PeerInterface;

public class PeerTest {
	public static void main(String[] args) throws NotBoundException, IOException {
		PeerInterface peer_interface = (PeerInterface) Naming.lookup("rmi://localhost/DBS_TEST");//args[0]); // Need to retouch this

		peer_interface.backup("C:\\Users\\Administrador\\Desktop\\rmi-sdis.txt", 1);

		/*switch (args[1].toLowerCase()) {
			case "backup":
				peer_interface.backup(args[2], Integer.valueOf(args[3]));
				break;
			case "restore":
				peer_interface.restore(args[2]);
				break;
			case "delete":
				peer_interface.delete(args[2]);
				break;
			case "reclaim":
				peer_interface.reclaim(Integer.valueOf(args[2]));
				break;
			case "state":
				System.out.println(peer_interface.state());
				break;
			case "stop":
				// MAYBE add this method to interface later
				break;
		}*/
	}
}
