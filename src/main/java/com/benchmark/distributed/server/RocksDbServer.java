package com.benchmark.distributed.server;

import com.benchmark.distributed.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Laptop 3: Database Server (i5-12500H, 16GB RAM, NVMe SSD)
 * This machine runs the actual RocksDB instance and serves DB requests.
 */
public class RocksDbServer {
    private static final Logger logger = Logger.getLogger(RocksDbServer.class.getName());
    private static final int PORT = 50052;
    private static final String DB_PATH = "C:\\distributed-rocksdb-data"; // Root level for speed/simplicity

    private Server server;
    private RocksDB db;

    private void start() throws IOException, RocksDBException {
        // Initialize RocksDB
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);

        // Laptop 3 Tuning (16GB RAM, Fast NVMe SSD)
        // High background threads for the 12-core i5
        options.setMaxBackgroundJobs(8);
        options.setWriteBufferSize(256 * 1024 * 1024); // 256MB write buffer
        options.setMaxWriteBufferNumber(4);

        File dbDir = new File(DB_PATH);
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }

        db = RocksDB.open(options, DB_PATH);
        logger.info("RocksDB initialized successfully at " + DB_PATH);

        // Start gRPC Server
        server = ServerBuilder.forPort(PORT)
                .addService(new DatabaseServiceImpl(db))
                // Maximize message size just in case, though key/vals are small
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build()
                .start();

        logger.info("DatabaseServer started, listening on " + PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            RocksDbServer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
        if (db != null) {
            db.close();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, RocksDBException {
        final RocksDbServer server = new RocksDbServer();
        server.start();
        server.blockUntilShutdown();
    }

    // High-performance direct RocksDB service
    static class DatabaseServiceImpl extends DatabaseServiceGrpc.DatabaseServiceImplBase {
        private final RocksDB db;
        // OPTIMIZATION: Disable WAL to prevent waiting on SSD syncs over the network
        private final WriteOptions writeOptions;

        DatabaseServiceImpl(RocksDB db) {
            this.db = db;
            this.writeOptions = new WriteOptions().setDisableWAL(true);
        }

        @Override
        public void putRecord(PutRequest req, StreamObserver<PutResponse> responseObserver) {
            try {
                // OPTIMIZATION: Use the write options with WAL disabled
                db.put(writeOptions, req.getKey().getBytes(), req.getValue().getBytes());
                PutResponse reply = PutResponse.newBuilder().setSuccess(true).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (RocksDBException e) {
                logger.severe("DB Write Error: " + e.getMessage());
                responseObserver.onError(e);
            }
        }

        @Override
        public void getRecord(GetRequest req, StreamObserver<GetResponse> responseObserver) {
            try {
                byte[] value = db.get(req.getKey().getBytes());
                GetResponse reply;
                if (value != null) {
                    reply = GetResponse.newBuilder().setFound(true).setValue(new String(value)).build();
                } else {
                    reply = GetResponse.newBuilder().setFound(false).build();
                }
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (RocksDBException e) {
                logger.severe("DB Read Error: " + e.getMessage());
                responseObserver.onError(e);
            }
        }
    }
}
