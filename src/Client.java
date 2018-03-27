import java.io.IOException;

import dbs.Peer;

public class Client {
	public static void main(String[] args) throws IOException {
		Peer peer = new Peer("1.0", "abc",
				"225.0.0.0", 12345,
				"225.0.0.0", 12346,
				"225.0.0.0", 12347);
	}
}
