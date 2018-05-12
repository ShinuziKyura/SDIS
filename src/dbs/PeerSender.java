package dbs;

import java.io.IOException;

import nio.channels.MulticastChannel;
import util.concurrent.LinkedInterruptibleQueue;

public class PeerSender implements Runnable {
	private Peer peer;
	private MulticastChannel channel;
	private LinkedInterruptibleQueue<byte[]> queue;

	PeerSender(Peer peer, MulticastChannel channel) {
		this.peer = peer;
		this.channel = channel;
		this.queue = new LinkedInterruptibleQueue<>();
	}

	@Override
	public void run() {
		while (peer.running.get()) {
			try {
				byte[] buffer = queue.take();

				channel.send(buffer);
			}
			catch (IOException | NullPointerException e) {
				// Probably terminating
			}
		}
	}

	void stop() {
		queue.interrupt();
	}

	void send(byte[] message) {
		queue.put(message);
	}
}
