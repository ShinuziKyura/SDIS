package dbs.peer;

import java.time.Instant;

import dbs.util.concurrent.LinkedInterruptibleQueue;

public class PeerLog implements Runnable {
	private Peer peer;
	private LinkedInterruptibleQueue<String> queue;

	PeerLog(Peer peer) {
		this.peer = peer;
		this.queue = new LinkedInterruptibleQueue<>();
	}

	@Override
	public void run() {
		while (peer.running.get()) {
			String message = queue.take();

			if (message == null) {
				break;
			}

			System.out.println(message);
		}
	}

	void stop() {
		queue.interrupt();
	}

	void print(String message) {
		queue.put(("\n" + Instant.now()).split("\\.")[0].replaceAll("T", " @ ") + message);
	}
}
