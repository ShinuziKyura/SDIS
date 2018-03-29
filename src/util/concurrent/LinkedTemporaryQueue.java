package util.concurrent;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinkedTemporaryQueue<E> extends LinkedTransferQueue<E> {
    private AtomicBoolean active = new AtomicBoolean(false);

    public void activate(long duration) {
        if (active.compareAndSet(false, true)) {
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    active.setRelease(false);
                }
            }, duration);
        }
    }

    @Override
    public void put(E object) {
        if (active.getAcquire()) {
            super.put(object);
        }
    }

    @Override
    public E poll() {
        E object = null;
        while (object == null && active.getAcquire()) {
            object = super.poll();
        }
        return object;
    }
}
