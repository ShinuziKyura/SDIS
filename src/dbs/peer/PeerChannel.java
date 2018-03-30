package dbs.peer;

import java.io.IOException;
import java.util.concurrent.LinkedTransferQueue;

import dbs.net.MulticastChannel;

public class PeerChannel implements Runnable {
	private Peer peer;
	private MulticastChannel socket;
	private LinkedTransferQueue<byte[]> queue;
	
	public PeerChannel(Peer peer, MulticastChannel socket) {
		this.peer = peer;
		this.socket = socket;
		this.queue = new LinkedTransferQueue<>();
	}
	
	@Override
	public void run() {
		while (peer.instances.get() >= 0) {
			try {
				byte[] buffer = socket.receive();
				queue.put(buffer);
			}
			catch (IOException e) {
				// Probably terminating, but we'll keep trying to receive()
                // until running is set to false
			}
		}
	}

	public void stop() {
	    while (!socket.isClosed()) {
            try {
                socket.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

	public LinkedTransferQueue<byte[]> queue(PeerQueue.ChannelQueue check) {
		check.getClass();
		return queue;
	}
}
