package util.concurrent;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

public class LinkedTransientQueue<E> extends LinkedTransferQueue<E> {
	private volatile long time = 0;
	private volatile long duration = 0;

    @Override
    public void put(E object) {
        if (duration - (System.nanoTime() - time) > 0) {
            super.put(object);
        }
    }

    @Override
    public E poll() {
    	E e = null;
        long poll_duration = duration - (System.nanoTime() - time);
        if (poll_duration > 0) {
            try {
			    e = super.poll(poll_duration, TimeUnit.NANOSECONDS);
		    }
		    catch (InterruptedException ee) {
            	return null;
		    }
	    }
        return e;
    }

	@Override
	public E poll(long duration, TimeUnit unit) {
		switch (unit) {
			case DAYS:
				duration *= 24;
			case HOURS:
				duration *= 60;
			case MINUTES:
				duration *= 60;
			case SECONDS:
				duration *= 1000;
			case MILLISECONDS:
				duration *= 1000;
			case MICROSECONDS:
				duration *= 1000;
			case NANOSECONDS:
				break;
		}

		E e = null;
		long poll_duration = this.duration - (System.nanoTime() - time);
		if (poll_duration > 0) {
			try {
				e = super.poll(duration < poll_duration ? duration : poll_duration, TimeUnit.NANOSECONDS);
			}
			catch (InterruptedException ee) {
				return null;
			}
		}
		return e;
	}

	public void clear(long duration, TimeUnit unit) {
		super.clear();

		switch (unit) {
			case DAYS:
				duration *= 24;
			case HOURS:
				duration *= 60;
			case MINUTES:
				duration *= 60;
			case SECONDS:
				duration *= 1000;
			case MILLISECONDS:
				duration *= 1000;
			case MICROSECONDS:
				duration *= 1000;
			case NANOSECONDS:
				break;
		}

		this.time = System.nanoTime();
		this.duration = duration;
	}
}
