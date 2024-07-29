package com.gfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gfs.models.Chunk;
import com.gfs.models.ChunkMetaData;
import com.gfs.models.ChunkServerInfo;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.net.*;
import java.util.*;

public class Master {

    private final Map<String, List<String>> fileChunkMapping = new HashMap<>();
    private final List<InetSocketAddress> chunkServers = new ArrayList<>();
    private final Map<String, List<ChunkServerInfo>> chunkServerMap = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void registerChunkServer(String address, int port) {
        chunkServers.add(new InetSocketAddress(address, port));
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(9000), 0);
        server.createContext("/getChunkMetaData", new GetChunkMetaDataHandler());
        server.createContext("/getChunkIds", new GetChunkIdsHandler());
        server.createContext("/getChunkServers", new GetChunkServers());
        server.setExecutor(null);
        server.start();
        System.out.println("MasterServer started on port 9000");
    }

    private class GetChunkMetaDataHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod()
                    .equalsIgnoreCase("POST")) {
                Chunk chunk = objectMapper.readValue(exchange.getRequestBody(), Chunk.class);
                ChunkMetaData chunkMetaData = getChunkMetaData(chunk.getFileName(), chunk.getNameSpace(),
                        chunk.getChunkIndex());
                String response = objectMapper.writeValueAsString(chunkMetaData);
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        }
    }

    private class GetChunkIdsHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod()
                    .equalsIgnoreCase("POST")) {
                Chunk chunk = objectMapper.readValue(exchange.getRequestBody(), Chunk.class);
                List<String> chunkIds = getChunkIds(chunk.getFileName(), chunk.getNameSpace());
                String response = objectMapper.writeValueAsString(chunkIds);
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        }
    }

    private class GetChunkServers implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod()
                    .equalsIgnoreCase("POST")) {
                String chunkId = new String(exchange.getRequestBody()
                        .readAllBytes());
                List<ChunkServerInfo> chunkServers = chunkServerMap.getOrDefault(chunkId, Collections.emptyList());

                String response = objectMapper.writeValueAsString(chunkServers);
                exchange.getResponseHeaders()
                        .set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        }
    }

    /*
    This method gets called in the write flow
    Master assigns chunk id and chunk servers to the chunk
    and updates the in mem maps
     */
    private ChunkMetaData getChunkMetaData(String fileName, String namespace, int chunkIndex) {
        String chunkId = UUID.randomUUID()
                .toString();
        assignServersToChunk(chunkId);
        String fileKey = namespace + "/" + fileName;
        fileChunkMapping.computeIfAbsent(fileKey, k -> new ArrayList<>())
                .add(chunkId);
        return new ChunkMetaData(chunkId, chunkServerMap.get(chunkId));
    }

    private void assignServersToChunk(String chunkId) {
        for (InetSocketAddress chunkServer : chunkServers) {
            chunkServerMap.computeIfAbsent(chunkId, k -> new ArrayList<>())
                    .add(new ChunkServerInfo(chunkServer.getHostName(), chunkServer.getPort()));
        }
    }

    private List<String> getChunkIds(String fileName, String namespace) {
        String fileKey = namespace + "/" + fileName;
        return fileChunkMapping.getOrDefault(fileKey, Collections.emptyList());
    }

    public static void main(String[] args) throws IOException {
        Master master = new Master();
        master.registerChunkServer("localhost", 9001);
        master.registerChunkServer("localhost", 9002);
        master.registerChunkServer("localhost", 9003);
        master.start();
    }
}
