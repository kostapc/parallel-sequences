package net.c0f3.commons.multiqueue;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * 27.04.2017
 * @author KostaPC
 * 2017 Infon ZED
 **/
public class MultiQueueTest {

    @Test
    public void testShortQueueSync() throws InterruptedException {
        int count = 100;
        long delay = -100;

        Executor executor = Executors.newFixedThreadPool(count);
        Logger LOG = Logger.getLogger(MultiQueueTest.class.getName());
        MultiQueue<Runnable> multiQueue = new MultiQueue<>(executor, LOG);

        List<String> sequence = new LinkedList<>();
        List<String> threads = new LinkedList<>();
        CountDownLatch door = new CountDownLatch(count);
        CountDownLatch startDoor = new CountDownLatch(count*2);
        CountDownLatch queuedDoor = new CountDownLatch(count);

        AtomicLong tasksCount = new AtomicLong(0);

        final List<Thread> runOnceList = new LinkedList<>();

        AtomicBoolean inUse = new AtomicBoolean(false);

        Random rnd = new Random();

        for (int i = 0; i < count; i++) {
            String key = String.valueOf("key_"+i);
            Thread thread = new Thread(()-> {
                startDoor.countDown();
                //LOG.info("task "+key+" waiting for notify... "+tasksCount.get());
                Runnable task = ()->{
                    Assert.assertFalse(inUse.get());
                    inUse.set(true);
                    if(delay!=0) {
                        try {
                            if(delay>0) {
                                Thread.sleep(delay);
                            } else {
                                Thread.sleep(rnd.nextInt((int) (delay*-1)));
                            }
                        } catch (InterruptedException e) {
                            Assert.fail(e.getMessage());
                        }
                    }
                    String sequenceInfo = "execution of "+key+" thread: "+Thread.currentThread().getName();
                    sequence.add(sequenceInfo);
                    threads.add(Thread.currentThread().getName());
                    door.countDown();
                    //LOG.info("\t>>> "+sequenceInfo+" (door: "+door.getCount()+")");
                    inUse.set(false);
                };
                synchronized (runOnceList) {
                    try {
                        runOnceList.wait();
                    } catch (InterruptedException e) {
                        Assert.fail(e.getMessage());
                        return;
                    }
                }
                multiQueue.put("same", task);
                tasksCount.incrementAndGet();
                queuedDoor.countDown();
            });
            runOnceList.add(thread);
            thread.start();
            startDoor.countDown();
        }

        LOG.info("waiting for threads init...");
        startDoor.await();
        LOG.info("starting all threads... tasks prepared: "+tasksCount.get());
        executor.execute(()->{
            for (int i = 0; i < count; i++) {
                synchronized (runOnceList) {
                    runOnceList.notifyAll();
                }
            }
        });

        LOG.info("waiting all tasks queued...");
        queuedDoor.await();
        Assert.assertEquals(count, tasksCount.get());

        LOG.info("waiting for all tasks done...");
        //door.await(count*delay+1000, TimeUnit.MILLISECONDS);
        door.await();
        Assert.assertEquals(count, threads.size());
        Assert.assertEquals(count, sequence.size());

        for (String s : sequence) {
            LOG.info("sequence: "+s);
        }
    }

}
