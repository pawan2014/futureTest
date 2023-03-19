package org.example;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Test1 {
    public void testcf() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);

        ArrayList<CompletableFuture<String>> fuList= new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int sam =i*10;
            CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    try {
                        System.out.println("inside process:"+Thread.currentThread().getName());
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    return "Result of the asynchronous computation"+sam;
                }
            }, executor);

            var nf  = future.thenApply(rec->{
                System.out.println("inside the 2nd processing login");
                return rec+"- 2nd process";
            });
            fuList.add(nf);
        }
        System.out.println("waiting for operation to complete");
        var data = fuList.stream().map(rec->rec.join()).collect(Collectors.toList());
        System.out.println(data);
        executor.shutdown();
    }
}
