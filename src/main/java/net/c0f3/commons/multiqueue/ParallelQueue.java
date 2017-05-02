package net.c0f3.commons.multiqueue;


import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 26.04.2017
 * @author KostaPC
 * 2017 Infon ZED
 **/

public class ParallelQueue<T extends Runnable> {

    private final Executor executor;
    private final Logger LOG;

    private final ConcurrentHashMap<String, ShortQueue> map = new ConcurrentHashMap<>();

    public ParallelQueue(Executor executor, Logger log) {
        this.executor = executor;
        this.LOG = log;
    }

    public void put(String key, T object) {
        AtomicBoolean newObject = new AtomicBoolean(false);

        ShortQueue shortQueue = map.computeIfAbsent(key, (k)->{
            ShortQueue queue = new ShortQueue(k);
            newObject.set(true);
            return queue;
        });

        shortQueue.put(object);
        if(newObject.get()) {
            executor.execute(shortQueue);
        }
        LOG.log(Level.FINEST,"task for key \""+key+"\" added to queue #"+shortQueue.id);
    }

    private static final AtomicLong queueId = new AtomicLong(0);

    private class ShortQueue implements Runnable {
        private final BlockingQueue<T> queue = new LinkedBlockingQueue<>();
        private final String key;
        private final long id;

        ShortQueue(String key) {
            this.key = key;
            this.id = queueId.incrementAndGet();
        }

        void put(T data) {
            try {
                queue.put(data);
            } catch (InterruptedException ignored) {}
        }

        @Override
        public void run() {
            for(T data = queue.poll(); data != null;) {
                try {
                    data.run();
                    data = queue.poll(10, TimeUnit.MILLISECONDS);
                    if(data==null) {
                        map.remove(key);
                        return;
                    }
                } catch (Exception e) {
                    LOG.log(Level.WARNING,"error while executing ShortQueue task",e);
                }
            }
            LOG.log(Level.FINEST,"short queue "+id+" exhausted for key \""+key+"\"... releasing thread \""+Thread.currentThread().getName()+"\"");
        }

    }

}
