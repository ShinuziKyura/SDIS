package dbs;

import dbs.util.PeerUtility;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class PeerProtocol implements Runnable {
	private Peer peer;
	private String[] message;

	public PeerProtocol(Peer peer, byte[] message) {
		this.peer = peer;
		this.message = new String(message).split("\r\n\r\n", 2)[0].split("[ ]+");
	}

	@Override
	public void run() {
		if (peer.processes.getAndIncrement() < 0) {
			peer.processes.decrementAndGet();
			return;
		}

		switch(message[0].toUpperCase()) {
			case "PUTCHUNK":

				break;
			case "STORED":

				break;
			case "GETCHUNK":

				break;
			case "CHUNK":

				break;
			case "DELETE":

				break;
			case "REMOVED":

				break;
		}

		peer.processes.decrementAndGet();
	}
}
