package dbs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class PeerListener implements Runnable
{
	private MulticastConnection socket;
	private AtomicBoolean running;
	
	public PeerListener(MulticastConnection socket)
	{
		this.socket = socket;
		this.running = new AtomicBoolean(true);
	}
	
	@Override
	public void run() {
		while (running.get())
		{
			try
			{
				String message = socket.receive();
			}
			catch (IOException e)
			{
				
			}
			
			// Process message
		}
	}
	
	public void stop() {
		this.running.set(false);
	}
}
