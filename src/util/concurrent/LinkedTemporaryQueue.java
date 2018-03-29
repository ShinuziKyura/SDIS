package util.concurrent;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinkedTemporaryQueue<E> extends LinkedTransferQueue<E> {
    private Timer timer;
    private AtomicBoolean active;

    private class TimerQueue extends TimerTask {
        public TimerQueue() {
            super();
        }

        @Override
        public void run() {
            active.setRelease(false);
        }
    }

    public LinkedTemporaryQueue() {
        super();
        timer = new Timer();
        active = new AtomicBoolean(false);
    }

    public void activate(long duration) {
        if (!active.compareAndExchange(false, true)) {
            timer.schedule(new TimerQueue(), duration);
        }
    }

    @Override
    public void put(E object) {
        if (active.getAcquire()) {
            super.put(object);
        }
    }

    @Override
    public E take() {
        E object = null;
        while (object == null && active.getAcquire()) {
            object = super.poll();
        }
        return object;
    }
}
