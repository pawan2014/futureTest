package org.example;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BlockingQFWriter{
    private BlockingQueue<String> stringBlockingQueue;
    private String fileName;
    private final String POISON="POISON";
    ExecutorService executor = Executors.newFixedThreadPool(1);

    public BlockingQFWriter(String fileName) {
        this.stringBlockingQueue = new ArrayBlockingQueue<>(1);
        this.fileName= fileName;
    }

    public void run() {
        try {
            while (true) {
                String take = stringBlockingQueue.take();
                if (take.equalsIgnoreCase(POISON)) {
                    System.out.println("[Completed] Writing to " + take + fileName);
                    break;
                }

                System.out.println("[FileWriter] Take : " + take + fileName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

    }

    public void start(){
            executor.submit(()->{
                run();
            });

    }
    public void write(FileWriter.S3Response subBatchResult) {
        stringBlockingQueue.offer(subBatchResult.getResponse().toString());
    }
    public void stop(){
        executor.shutdown();
    }

    public void sendPosion() {
        stringBlockingQueue.offer(POISON);
    }
}
