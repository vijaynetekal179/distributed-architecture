package com.benchmark.distributed.client;

import com.benchmark.distributed.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Laptop 1: Benchmark Client (i3-1115G4, 4GB RAM)
 * Generates load using High-Performance Asynchronous Streaming over a Network.
 */
public class BenchmarkClient {
    private static String apiProxyHost = "192.168.0.211";
    private static final int API_PROXY_PORT = 50051;

    private static final int NUM_OPERATIONS = 1_000_000; // Increased to 1 Million items for stress-test!
    // Laptop 1 has 4GB RAM. We cannot push 1 million requests into memory at the
    // exact same moment.
    // We will throttle "In-Flight" network requests to a maximum of 15,000 parallel
    // streams.
    private static final int MAX_CONCURRENT_REQUESTS = 15000;

    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) {
            apiProxyHost = args[0];
        } else {
            System.out.println("⚠️ No API Proxy IP provided. Defaulting to: " + apiProxyHost);
        }

        System.out.println("🚀 Starting (ASYNC) Client Load Generator (Laptop 1)");
        System.out.println("- Target: " + apiProxyHost + ":" + API_PROXY_PORT);
        System.out.println("- Max In-Flight Streams: " + MAX_CONCURRENT_REQUESTS);
        System.out.println("- Total Operations: " + NUM_OPERATIONS);

        ManagedChannel channel = ManagedChannelBuilder.forAddress(apiProxyHost, API_PROXY_PORT)
                .usePlaintext()
                // Networking tuning for sending mass amounts of data seamlessly
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build();

        // **OPTIMIZATION 1:** Switch to non-blocking Asynchronous Stub
        ApiServiceGrpc.ApiServiceStub asyncStub = ApiServiceGrpc.newStub(channel);

        // Control the number of parallel network requests so we don't blow up Laptop
        // 1's JVM
        Semaphore inFlightPermits = new Semaphore(MAX_CONCURRENT_REQUESTS);
        CountDownLatch latch = new CountDownLatch(NUM_OPERATIONS);

        AtomicLong totalLatencyNs = new AtomicLong(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        // **OPTIMIZATION 2:** Use a single aggressive loop instead of ThreadPool
        // We just dispatch tasks over the network instantly!
        System.out.println("\n🔥 Pumping " + NUM_OPERATIONS + " Async streams across the network...");

        for (int i = 0; i < NUM_OPERATIONS; i++) {
            // Wait for permission if we have hit the 15,000 threshold
            inFlightPermits.acquire();

            final int index = i;
            long opStart = System.nanoTime();

            PutRequest request = PutRequest.newBuilder()
                    .setKey("key" + index)
                    .setValue("value" + index + "_payload_data_padding_to_simulate_real_load_1234567890")
                    .build();

            // Fire-and-forget over the network
            asyncStub.putRecord(request, new StreamObserver<PutResponse>() {
                @Override
                public void onNext(PutResponse response) {
                    if (response.getSuccess()) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    failCount.incrementAndGet();
                    latch.countDown();
                    inFlightPermits.release();
                    totalLatencyNs.addAndGet(System.nanoTime() - opStart);
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                    inFlightPermits.release(); // Freed a network slot, let another one run!
                    totalLatencyNs.addAndGet(System.nanoTime() - opStart);
                }
            });
        }

        System.out.println("⏳ Waiting for " + NUM_OPERATIONS + " operations to return from Laptop 3...");
        latch.await();
        long endTime = System.currentTimeMillis();

        channel.shutdown();

        // Calculate and Print Metrics
        long durationMs = endTime - startTime;
        double durationSec = durationMs / 1000.0;
        double tps = NUM_OPERATIONS / durationSec;
        double avgLatencyMs = (totalLatencyNs.get() / (double) NUM_OPERATIONS) / 1_000_000.0;

        System.out.println("\n📊 --- DISTRIBUTED BENCHMARK RESULTS ---");
        System.out.println("Total Time: " + durationSec + " seconds");
        System.out.println("Total Operations: " + NUM_OPERATIONS);
        System.out.println("Successful Ops: " + successCount.get());
        System.out.println("Failed Ops: " + failCount.get());
        System.out.println("----------------------------------");
        System.out.printf("⭐ Throughput (TPS): %.2f ops/sec\n", tps);
        System.out.printf("⏱️ Average Latency: %.2f ms/op\n", avgLatencyMs);
        System.out.println("----------------------------------");
        System.out.println(
                "\nNote: End-To-End Latency = Laptop 1 -> Laptop 2 -> Laptop 3 -> SSD -> Laptop 2 -> Laptop 1");
    }
}
