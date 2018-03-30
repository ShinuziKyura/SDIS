package util.concurrent;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

public class LinkedTransientQueue<E> extends LinkedTransferQueue<E> {
	private volatile long init_time = 0;
	private volatile long duration = 0;

	public void init(long duration, TimeUnit unit) {
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

		this.init_time = System.nanoTime();
		this.duration = duration;
	}

    @Override
    public void put(E object) {
        if (duration - (System.nanoTime() - init_time) > 0) {
            super.put(object);
        }
    }

    @Override
    public E take() {
    	E e = null;
        long poll_duration = duration - (System.nanoTime() - init_time);
        if (poll_duration > 0) {
            try {
			    e = super.poll(poll_duration, TimeUnit.NANOSECONDS);
		    }
		    catch (InterruptedException ee) {
		    }
	    }
        return e;
    }
}
