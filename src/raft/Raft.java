package raft;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class Raft<T> {
	private class Log {
		private T entry;
		private Long term = 0L;
	}

	private class Listener implements Runnable {
		@Override
		public void run() {
			while (running.get()) {
				try {
					executor.execute(new Communicator(socket.accept()));
				} catch (IOException e) {
					//
				}
			}
		}
	}

	private class Communicator implements Runnable {
		private Socket socket;

		private Communicator(Socket socket) {
			this.socket = socket;
		}

		@Override
		public void run() {

		}
	}

//	Persistent state
	private Long currentTerm = 0L;
//	private ID votedFor;
	private Log[] log;

//	Volatile state
	private Long commitIndex = 0L;
	private Long lastApplied = 0L;

//	Leader state
	private Long[] nextIndex;
	private Long[] matchIndex;

//	Required variables
	private ServerSocket socket;
	private AtomicBoolean running;
	private ThreadPoolExecutor executor;

	public Raft(int port) throws IOException {
		this.socket = new ServerSocket(port);
		this.running = new AtomicBoolean(true);
		this.executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
	}

	public Raft() throws IOException {
		this(0);
	}

}
