package dbs.peer;

import java.io.IOException;

import dbs.nio.channels.MulticastChannel;
import dbs.util.concurrent.LinkedInterruptibleQueue;

public class PeerReceiver implements Runnable {
	private Peer peer;
	private MulticastChannel channel;
	private LinkedInterruptibleQueue<byte[]> queue;
	
	PeerReceiver(Peer peer, MulticastChannel channel) {
		this.peer = peer;
		this.channel = channel;
		this.queue = new LinkedInterruptibleQueue<>();
	}
	
	@Override
	public void run() {
		while (peer.running.get()) {
			try {
				byte[] buffer = channel.receive();

				queue.put(buffer);
			}
			catch (IOException | NullPointerException e) {
				// Probably terminating
			}
		}
	}

	void stop() {
		queue.interrupt();
	}

    byte[] receive() {
		return queue.take();
    }
}
