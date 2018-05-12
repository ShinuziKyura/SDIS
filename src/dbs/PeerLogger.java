package dbs;

import java.time.Instant;

import util.concurrent.LinkedInterruptibleQueue;

public class PeerLogger implements Runnable {
	private Peer peer;
	private LinkedInterruptibleQueue<String> queue;

	PeerLogger(Peer peer) {
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
