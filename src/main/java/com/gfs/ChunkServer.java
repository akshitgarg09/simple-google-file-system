package com.gfs;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.*;

public class ChunkServer {

    private String chunkDir;

    public void start(int port, int serverId) throws IOException {
        chunkDir = "chunks/" + serverId;
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/writeChunk", new WriteChunkHandler());
        server.createContext("/readChunk", new ReadChunkHandler());
        server.setExecutor(null);
        server.start();
        System.out.println("ChunkServer started on port " + port);
    }

    private class WriteChunkHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod()
                    .equalsIgnoreCase("POST")) {
                String chunkId = exchange.getRequestHeaders()
                        .getFirst("chunkId");
                byte[] data = exchange.getRequestBody()
                        .readAllBytes();

                File chunkFile = new File(chunkDir, chunkId);
                File parentDir = chunkFile.getParentFile();

                if (!parentDir.exists()) {
                    parentDir.mkdirs();
                }

                try (FileOutputStream fos = new FileOutputStream(chunkFile)) {
                    fos.write(data);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }

                String response = "Chunk written";
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        }
    }

    private class ReadChunkHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod()
                    .equalsIgnoreCase("GET")) {
                String chunkId = exchange.getRequestURI()
                        .getQuery()
                        .split("=")[1];

                File chunkFile = new File(chunkDir, chunkId);

                if (chunkFile.exists()) {
                    byte[] data = Files.readAllBytes(chunkFile.toPath());
                    exchange.sendResponseHeaders(200, data.length);
                    exchange.getResponseBody()
                            .write(data);
                } else {
                    exchange.sendResponseHeaders(404, -1);
                }

                exchange.getResponseBody()
                        .close();

                System.out.println("Chunk read");
            }
        }
    }

    public static void main(String[] args) throws IOException {
        ChunkServer chunkServer = new ChunkServer();
        chunkServer.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    }
}

