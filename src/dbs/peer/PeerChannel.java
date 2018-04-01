package dbs.peer;

import java.io.IOException;
import java.util.concurrent.LinkedTransferQueue;

import dbs.nio.channels.MulticastChannel;

public class PeerChannel implements Runnable {
	private Peer peer;
	private MulticastChannel channel;
	private LinkedTransferQueue<byte[]> queue;
	
	PeerChannel(Peer peer, MulticastChannel channel) {
		this.peer = peer;
		this.channel = channel;
		this.queue = new LinkedTransferQueue<>();
	}
	
	@Override
	public void run() {
		while (peer.running.get()) {
			try {
				byte[] buffer = channel.receive();

				queue.put(buffer);
			}
			catch (IOException e) {
				// Probably terminating
			}
		}
	}

	void stop() {
	    while (!channel.isClosed()) {
            try {
                channel.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

	LinkedTransferQueue<byte[]> queue(PeerQueue.ChannelQueue check) {
		check.getClass();
		return queue;
	}
}
