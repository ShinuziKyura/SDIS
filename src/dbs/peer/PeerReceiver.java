package dbs.peer;

import java.io.IOException;

import dbs.nio.channels.MulticastChannel;
import dbs.util.concurrent.LinkedInterruptibleQueue;

import static dbs.nio.channels.DatagramChannel.DatagramPackage;

public class PeerReceiver implements Runnable {
	private Peer peer;
	private MulticastChannel channel;
	private LinkedInterruptibleQueue<DatagramPackage> queue;
	
	PeerReceiver(Peer peer, MulticastChannel channel) {
		this.peer = peer;
		this.channel = channel;
		this.queue = new LinkedInterruptibleQueue<>();
	}
	
	@Override
	public void run() {
		while (peer.running.get()) {
			try {
				DatagramPackage buffer = channel.receive();

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

    DatagramPackage receive() {
		return queue.take();
    }
}
