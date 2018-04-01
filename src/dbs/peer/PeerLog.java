package dbs.peer;

import java.time.Instant;
import java.util.concurrent.LinkedTransferQueue;

public class PeerLog implements Runnable {
	private Peer peer;
	private LinkedTransferQueue<String> queue;

	PeerLog(Peer peer) {
		this.peer = peer;
		this.queue = new LinkedTransferQueue<>();
	}

	@Override
	public void run() {
		while (peer.running.get()) {
			try {
				String message = queue.take();

				if (message.equals("STOP")) {
					break;
				}

				System.out.println(message);
			}
			catch (InterruptedException e) {
			}
		}
	}

	void stop() {
		queue.put("STOP");
	}

	void print(String message) {
		queue.put(("\n" + Instant.now()).split("\\.")[0].replaceAll("T", " @ ") + message);
	}
}
