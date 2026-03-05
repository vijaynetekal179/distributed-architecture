package com.benchmark.distributed.proxy;

import com.benchmark.distributed.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Laptop 2: API Proxy Server (Ryzen 5 4600H, 8GB RAM)
 * This machine acts as a middleman. It receives traffic from the client, and
 * forwards it to the DB server.
 */
public class ApiProxyServer {
    private static final Logger logger = Logger.getLogger(ApiProxyServer.class.getName());

    private static final int LISTEN_PORT = 50051;
    // Defaulting to loopback for local testing, but this is overridden via command
    // line args
    private static String dbServerHost = "192.168.0.130";
    private static final int DB_SERVER_PORT = 50052;

    private Server server;
    private ManagedChannel dbChannel;

    private void start() throws IOException {
        // 1. Create a channel to Laptop 3 (Database Tier)
        dbChannel = ManagedChannelBuilder.forAddress(dbServerHost, DB_SERVER_PORT)
                .usePlaintext() // No TLS for raw speed internally
                .build();

        DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub = DatabaseServiceGrpc.newStub(dbChannel);

        // 2. Start accepting traffic from Laptop 1 (Client Tier)
        server = ServerBuilder.forPort(LISTEN_PORT)
                .addService(new ApiProxyServiceImpl(asyncDbStub))
                // Max out connections/threads for the 6-core Ryzen
                // .maxConcurrentCallsPerConnection(10000) // Not available in this grpc version
                // 1.59.0
                .build()
                .start();

        logger.info("ApiProxyServer started, listening on " + LISTEN_PORT);
        logger.info("Forwarding proxy traffic to DB Tier at " + dbServerHost + ":" + DB_SERVER_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC proxy server");
            ApiProxyServer.this.stop();
            System.err.println("*** proxy server shut down");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
        if (dbChannel != null) {
            dbChannel.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Pass DB IP via: java -jar app.jar <DB_IP>
        if (args.length > 0) {
            dbServerHost = args[0];
        } else {
            logger.warning(
                    "No DB Server IP provided. Defaulting to localhost. For distributed deployment, provide IP as first argument.");
        }

        final ApiProxyServer server = new ApiProxyServer();
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Non-blocking proxy implementation to max out TPS without eating too much of
     * Laptop 2's 8GB RAM.
     */
    static class ApiProxyServiceImpl extends ApiServiceGrpc.ApiServiceImplBase {
        private final DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub;

        ApiProxyServiceImpl(DatabaseServiceGrpc.DatabaseServiceStub asyncDbStub) {
            this.asyncDbStub = asyncDbStub;
        }

        @Override
        public void putRecord(PutRequest req, StreamObserver<PutResponse> responseObserver) {
            // Forward request asynchronously to Laptop 3
            asyncDbStub.putRecord(req, new StreamObserver<PutResponse>() {
                @Override
                public void onNext(PutResponse value) {
                    // Send Laptop 3's response back to Laptop 1
                    responseObserver.onNext(value);
                }

                @Override
                public void onError(Throwable t) {
                    logger.severe("Error forwarding Put to DB: " + t.getMessage());
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void getRecord(GetRequest req, StreamObserver<GetResponse> responseObserver) {
            // Forward request asynchronously to Laptop 3
            asyncDbStub.getRecord(req, new StreamObserver<GetResponse>() {
                @Override
                public void onNext(GetResponse value) {
                    // Send Laptop 3's response back to Laptop 1
                    responseObserver.onNext(value);
                }

                @Override
                public void onError(Throwable t) {
                    logger.severe("Error forwarding Get to DB: " + t.getMessage());
                    responseObserver.onError(t);
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            });
        }
    }
}
