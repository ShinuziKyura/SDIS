package dbs.peer.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.NotBoundException;

import dbs.peer.PeerInterface;
import dbs.peer.PeerUtility;
import dbs.rmi.RemoteFunction;

public class PeerTest {
	public static void main(String[] args) throws NotBoundException, IOException {
		//* Single-comment this line to activate the check
	    if (args.length < 2) /**/ {
	    	System.err.println("\n######## Distributed Backup Service ########" +
                               "\nPeerTest must be called with the following arguments:" +
                               "\n\t<gen_peer_ap> <sub_protocol> [<args> ...]" +
                               "\n\tWhere:" +
                               "\n\t\t<gen_peer_ap> is one of the following values:" +
                               "\n\t\t\t<host>/<peer_ap>" +
                               "\n\t\t\t<peer_ap>" +
                               "\n\t\t\tAnd where:" +
                               "\n\t\t\t\t<host> is either an IPv4 address or a domain name" +
                               "\n\t\t\t\t<peer_ap> is an identifier that can contain all lower non-zero-width ASCII characters except \"/\" (slash)" +
                               "\n\t\t<sub_protocol> [<args> ...] is one of the following values:" +
                               "\n\t\t\tBACKUP <file_path> <rep_deg>" +
                               "\n\t\t\tRESTORE <file_name>" +
                               "\n\t\t\tDELETE <file_name>" +
                               "\n\t\t\tRECLAIM <size>" +
                               "\n\t\t\tSTATE" +
                               "\n\t\t\tAnd where:" +
                               "\n\t\t\t\t<file_path> is a Windows or POSIX, relative or absolute, path to a file" +
                               "\n\t\t\t\t<rep_deg> is a single digit between 1 and 9" +
                               "\n\t\t\t\t<file_name> is a name of a file" +
                               "\n\t\t\t\t<byte_size> is a number between 0 and 9223372036854775807");
            System.exit(1);
        }
		//*
		PeerInterface peer_interface = (PeerInterface) Naming.lookup("rmi://localhost/DBS_TEST");//args[0]); // TODO (look for last slash with regex)

		switch (args[1].toUpperCase()) {
			case "BACKUP":
                Path filepath = Paths.get(args[2]);

                String filename = filepath.getFileName().toString();
                String fileID = PeerUtility.generateFileID(filepath);
                byte[] file = Files.readAllBytes(filepath);

                @SuppressWarnings("unchecked")
				Integer backup = ((RemoteFunction<Integer>) peer_interface.backup(filename, fileID, file, Integer.valueOf(args[3]))).call();
				System.exit(backup);
			case "RESTORE":
				@SuppressWarnings("unchecked")
				Integer restore = ((RemoteFunction<Integer>) peer_interface.restore(args[2])).call();
				System.exit(restore);
			case "DELETE":
				@SuppressWarnings("unchecked")
				Integer delete = ((RemoteFunction<Integer>) peer_interface.delete(args[2])).call();
				System.exit(delete);
			case "RECLAIM":
				@SuppressWarnings("unchecked")
				Integer reclaim = ((RemoteFunction<Integer>) peer_interface.reclaim(Integer.valueOf(args[2]))).call();
				System.exit(reclaim);
			case "STATE":
				String state = peer_interface.state();
				System.out.println(state);
				break;
			case "STOP":
				peer_interface.stop();
				break;
		}
		//*/
	}
}
