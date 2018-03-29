package dbs;

import java.io.IOException;
import java.util.concurrent.LinkedTransferQueue;

import dbs.util.PeerUtility;
import net.MulticastChannel;

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
		while (peer.running.get()) {
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

	public LinkedTransferQueue<byte[]> queue() {
		return queue;
	}
}
