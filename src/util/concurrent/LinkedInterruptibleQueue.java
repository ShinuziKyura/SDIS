package util.concurrent;

import java.util.HashSet;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;


public class LinkedInterruptibleQueue<E> extends LinkedTransferQueue<E> {
	private boolean interrupted;
	private HashSet<Thread> blocked;
	private WriteLock exclusive_access;
	private ReadLock shared_access;

	public LinkedInterruptibleQueue() {
		interrupted = false;
		blocked = new HashSet<>();
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		exclusive_access = lock.writeLock();
		shared_access = lock.readLock();
	}

	@Override
	public void put(E object) {
		shared_access.lock();
		if (!interrupted) {
			super.put(object);
		}
		shared_access.unlock();
	}

	@Override
	public E take() {
		E e = null;
		shared_access.lock();
		if (!interrupted) {
			blocked.add(Thread.currentThread());
			shared_access.unlock();
			try {
				e = super.take();
			}
			catch (InterruptedException ee) {
				// Interrupt called
			}
			shared_access.lock();
			blocked.remove(Thread.currentThread());
		}
		shared_access.unlock();
		while(Thread.interrupted());
		return e;
	}

	public void interrupt() {
		exclusive_access.lock();
		if (!interrupted) {
			super.clear();
			for (Thread thread : blocked) {
				thread.interrupt();
			}
			interrupted = true;
		}
		exclusive_access.unlock();
	}
}
