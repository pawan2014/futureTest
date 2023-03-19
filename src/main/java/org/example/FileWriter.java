package org.example;

import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Batch
 *  SubBatch1
 *      File
 */
public class FileWriter {

    public void prcoessRecords() {
        var allEntities = getDummy();
        var batchOfAccounts = partition(allEntities);
        System.out.println("Start-Processing # of Account batches size = "+batchOfAccounts.size());
        for (AccountBatch batch : batchOfAccounts) {
            System.out.println("--------------------------");
            System.out.println("->"+batch.getBatchId()+", start batch processing");

            BlockingQFWriter blockingQFWriter = new BlockingQFWriter("File"+batch.getBatchId());
            blockingQFWriter.start();
            processBatchOfAccounts(batch,blockingQFWriter);
            blockingQFWriter.sendPosion();
            blockingQFWriter.stop();
            System.out.println("->"+batch.getBatchId()+", finish batch processing");
        }
        System.out.println("Complete-Processing # of batches = "+batchOfAccounts.size());
    }

    private void processBatchOfAccounts(AccountBatch batch,BlockingQFWriter blockingQFWriter) {
        AtomicInteger subBatchCounter = new AtomicInteger(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            ArrayList<CompletableFuture<S3Response>> accountF = new ArrayList<>();
            // Loop
            for (SubBatch subBatch : batch.getSubBatches()) {
                System.out.println("->"+batch.getBatchId()+","+subBatch.subBatchId+", Sub Batch submitted for processing");
                // Submit SubBatch
                var subBatchFuture = CompletableFuture.supplyAsync(()->{
                    return processSubBatch(subBatch,batch.getBatchId(),subBatchCounter.getAndIncrement());
                },executor);
                // write to file
                var finalF = subBatchFuture.thenApply(subBatchResult->{
                    System.out.println("->"+subBatchResult.getBatchId()+","+subBatchResult.getSubBatchId()+", with"+Thread.currentThread().getName()+" writing to File");
                    blockingQFWriter.write(subBatchResult);
                    return subBatchResult;
                });
                accountF.add(finalF);
            }
            System.out.println("6.Waiting for Account Batch "+batch.getBatchId()+"to be completed");
            var res = accountF.stream().map(CompletableFuture::join).collect(Collectors.toList());

        } finally {
            executor.shutdown();
        }

    }

    private S3Response processSubBatch(SubBatch value, int batchId, int subBatchCounter) {
        // Submit records and join. Send the final result as List
        System.out.println("->"+value.getBatchId()+","+value.subBatchId+", with"+Thread.currentThread().getName()+" sub batch started");
        var future = value.getS3FileInfoList().stream().map(s3FileInfo->dummyDownloader(s3FileInfo))
                .collect(Collectors.toList());
        var result =  future.stream().map(CompletableFuture::join).collect(Collectors.toList());
        System.out.println("->"+value.getBatchId()+","+value.subBatchId+", with"+Thread.currentThread().getName()+" sub batch completed");
        return S3Response.builder().batchId(value.getBatchId()).subBatchId(value.getSubBatchId()).response(result).build();
    }

    private CompletableFuture<String> dummyDownloader(S3FileInfo s3FileInfo) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                System.out.println("Downloading file..."+Thread.currentThread().getName()+"->"+s3FileInfo.getDetailName());
                TimeUnit.SECONDS.sleep(2);

            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return "Download Result->"+s3FileInfo.getDetailName();
        });
    }

    private List<AccountBatch> partition(List<Entity> allEntities) {
        final int chunkSize = 5;
        final AtomicInteger counter = new AtomicInteger();
        final AtomicInteger batch = new AtomicInteger(1);
        final AtomicInteger subbatch = new AtomicInteger(1);
        return allEntities.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .values().stream().map(listOfEntites -> {
                    int bid = batch.getAndIncrement();
                   // Map
                    var sublist =genericP(S3FileInfo.asList(listOfEntites));
                    // Create SubBatch
                    var subBatch = sublist.values().stream().map(m->SubBatch.builder()
                            .subBatchId(subbatch.getAndIncrement())
                            .batchId(bid)
                            .s3FileInfoList(m)
                            .build()).collect(Collectors.toList());
                    return new AccountBatch(bid, subBatch);
                }).collect(Collectors.toList());
    }

    private <T> Map<Integer, List<T>> genericP(List<T> allEntities) {
        final int chunkSize = 2;
        final AtomicInteger counter = new AtomicInteger();
        return allEntities.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize));

    }

    public List<Entity> getDummy() {
        return Stream.of(
                new Entity("Test 1"),
                new Entity("Test 3"),
                new Entity("Test 2"),
                new Entity("Test 4"),
                new Entity("Test 5"),
                new Entity("Test 6"),
                new Entity("Test 7"),
                new Entity("Test 8"),
                new Entity("Test 9"),
                new Entity("Test 10"),
                new Entity("Test 11"),
                new Entity("Test 12"),
                new Entity("Test 13"),
                new Entity("Test 14"),
                new Entity("Test 15"),
                new Entity("Test 16"),
                new Entity("Test 17"),
                new Entity("Test 18"),
                new Entity("Test 19"),
                new Entity("Test 20")
        ).collect(Collectors.toList());

    }

    @Builder
    @Getter
    static class S3Response{
        int batchId;
        int subBatchId;
        List<String> response;


    }
    @Getter
    class AccountBatch {
        public AccountBatch(int batchId, List<SubBatch> subBatches) {
            this.batchId = batchId;
            this.subBatches = subBatches;
        }
        int batchId;
        List<SubBatch> subBatches;
    }

    @Builder
    @Getter
    static class SubBatch{
        int batchId;
        int subBatchId;
        List<S3FileInfo> s3FileInfoList;
    }


    @Getter
    static class S3FileInfo {
        String batchId;
        String detailName;
        String summaryName;

        public S3FileInfo(String batchId, String detailName, String summaryName) {
            this.batchId = batchId;
            this.detailName = detailName;
            this.summaryName = summaryName;
        }

        @Override
        public String toString() {
            return "{" +
                    "batchId='" + batchId + '\'' +
                    ", detailName='" + detailName + '\'' +
                    ", summaryName='" + summaryName + '\'' +
                    '}';
        }

        public static List<S3FileInfo> asList(List<Entity> rec) {
            return rec.stream().map(entity -> new S3FileInfo(null, entity.getEntityName(), entity.getEntityName())).collect(Collectors.toList());
        }

    }


    @Getter
    static class Entity {
        public Entity(String entityName) {
            this.entityName = entityName;
        }

        String entityName;
    }
}

