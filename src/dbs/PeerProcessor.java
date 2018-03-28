package dbs;

import java.util.concurrent.LinkedTransferQueue;

public class PeerProcessor implements Runnable {
    private Peer peer;
    private LinkedTransferQueue<byte[]> queue;

    public PeerProcessor(Peer peer, LinkedTransferQueue<byte[]> queue) {
        this.peer = peer;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (peer.running.get()) {
            try {
                byte[] buffer = queue.take();
                String[] message_header = new String(buffer).split("\r\n\r\n", 2)[0].split("[ ]+");
                if (!message_header[2].equals(peer.id)) {
                    switch(message_header[0].toUpperCase()) {
                        case "PUTCHUNK": // MDB

                            break;
                        case "STORED": // MC

                            break;
                        case "GETCHUNK": // MC

                            break;
                        case "CHUNK": // MDR

                            break;
                        case "DELETE": // MC

                            break;
                        case "REMOVED": // MC

                            break;
                    }
                }
            }
            catch (InterruptedException ee) {
                // Shouldn't realistically happen
            }
        }
    }
}
