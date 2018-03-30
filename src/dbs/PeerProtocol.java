package dbs;

import util.GenericArrays;

public class PeerProtocol implements Runnable {
	private Peer peer;
	private String[] header;
	private byte[] body;

	public PeerProtocol(Peer peer, byte[] message) {
		this.peer = peer;
		String header = new String(message).split("\r\n\r\n", 2)[0];
		this.header = header.split("[ ]+");
		byte[] body = GenericArrays.split(message, header.length())[1];
		this.body = body.length > 4 ? java.util.Arrays.copyOfRange(body, 4, body.length) : null;
	}

	@Override
	public void run() {
		if (peer.processes.getAndIncrement() < 0) {
			peer.processes.decrementAndGet();
			return;
		}

		switch(header[0].toUpperCase()) {
			case "PUTCHUNK":
				backup();
				break;
			case "GETCHUNK":

				break;
			case "DELETE":

				break;
			case "REMOVED":

				break;
		}

		peer.processes.decrementAndGet();
	}

	public void backup() {

	}
}
