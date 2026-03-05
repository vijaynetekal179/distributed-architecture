package com.benchmark.distributed.client;

import com.benchmark.distributed.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Laptop 1: Benchmark Client (i3-1115G4, 4GB RAM)
 * Generates load and tracks latencies.
 */
public class BenchmarkClient {
    private static String apiProxyHost = "192.168.0.211";
    private static final int API_PROXY_PORT = 50051;

    // Laptop 1 Tuning: It only has an i3 and 4GB RAM.
    // If we use too many threads, it will crash. 64 concurrent streams is safe but
    // high enough to generate good load.
    private static final int NUM_THREADS = 64;
    private static final int NUM_OPERATIONS = 500_000; // Moderate target batch

    public static void main(String[] args) throws InterruptedException {
        // Pass Proxy API IP via: java -jar app.jar <API_IP>
        if (args.length > 0) {
            apiProxyHost = args[0];
        } else {
            System.out.println("⚠️ No API Proxy IP provided. Defaulting to localhost.");
        }

        System.out.println("🚀 Starting Client Load Generator (Laptop 1)");
        System.out.println("- Target: " + apiProxyHost + ":" + API_PROXY_PORT);
        System.out.println("- Threads: " + NUM_THREADS);
        System.out.println("- Total Operations: " + NUM_OPERATIONS);

        ManagedChannel channel = ManagedChannelBuilder.forAddress(apiProxyHost, API_PROXY_PORT)
                .usePlaintext()
                // Tuning for high throughput connection
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build();

        // Use blocking stub for the thread-pool executor model
        ApiServiceGrpc.ApiServiceBlockingStub blockingStub = ApiServiceGrpc.newBlockingStub(channel);

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_OPERATIONS);

        AtomicLong totalLatencyNs = new AtomicLong(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            final int index = i;
            executor.submit(() -> {
                long opStart = System.nanoTime();
                try {
                    PutRequest request = PutRequest.newBuilder()
                            .setKey("key" + index)
                            .setValue("value" + index + "_payload_data_padding_to_simulate_real_load_1234567890")
                            .build();

                    PutResponse response = blockingStub.putRecord(request);
                    if (response.getSuccess()) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                    long opEnd = System.nanoTime();
                    totalLatencyNs.addAndGet(opEnd - opStart);
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        System.out.println("⏳ Waiting for " + NUM_OPERATIONS + " operations to complete...");
        latch.await();
        long endTime = System.currentTimeMillis();

        executor.shutdown();
        channel.shutdown();

        // Calculate and Print Metrics
        long durationMs = endTime - startTime;
        double durationSec = durationMs / 1000.0;
        double tps = NUM_OPERATIONS / durationSec;
        double avgLatencyMs = (totalLatencyNs.get() / (double) NUM_OPERATIONS) / 1_000_000.0;

        System.out.println("\n📊 --- BENCHMARK RESULTS ---");
        System.out.println("Total Time: " + durationSec + " seconds");
        System.out.println("Total Operations: " + NUM_OPERATIONS);
        System.out.println("Successful Ops: " + successCount.get());
        System.out.println("Failed Ops: " + failCount.get());
        System.out.println("----------------------------------");
        System.out.printf("⭐ Throughput (TPS): %.2f ops/sec\n", tps);
        System.out.printf("⏱️ Average Latency: %.2f ms/op\n", avgLatencyMs);
        System.out.println("----------------------------------");
        System.out.println(
                "\nNote: This is end-to-end latency including Machine 1 -> Machine 2 -> Machine 3 network hops.");
    }
}
