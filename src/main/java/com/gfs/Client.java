package com.gfs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import com.gfs.models.Chunk;
import com.gfs.models.ChunkMetaData;
import com.gfs.models.ChunkServerInfo;

public class Client {

    private static final String MASTER_SERVER_HOST = "localhost";
    private static final int MASTER_SERVER_PORT = 9000;
    private static final String DOWNLOAD_DIR = "download";
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public Client() {
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    public void uploadFile(String filename, String namespace) throws IOException, InterruptedException {
        byte[] data = Files.readAllBytes(Paths.get(namespace + "/" + filename));
        int chunkSize = 1024 * 1024; // 1 MB chunks
        int numChunks = (data.length + chunkSize - 1) / chunkSize;

        // split the file into chunks of equal size
        for (int i = 0; i < numChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, data.length);
            byte[] chunkData = new byte[end - start];
            System.arraycopy(data, start, chunkData, 0, end - start);

            // Call Master server to get chunk meta data
            Chunk chunk = new Chunk(filename, namespace, i);
            HttpRequest chunkInfoRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + MASTER_SERVER_HOST + ":" + MASTER_SERVER_PORT + "/getChunkMetaData"))
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(chunk)))
                    .header("Content-Type", "application/json")
                    .build();

            HttpResponse<String> chunkInfoResponse = httpClient.send(chunkInfoRequest,
                    HttpResponse.BodyHandlers.ofString());
            ChunkMetaData chunkMetaData = objectMapper.readValue(chunkInfoResponse.body(), ChunkMetaData.class);

            // write chunk data to each chunk server replica
            for (ChunkServerInfo chunkServerInfo : chunkMetaData.getChunkServers()) {
                String chunkServerUrl = "http://" + chunkServerInfo.host + ":" + chunkServerInfo.port + "/writeChunk";
                HttpRequest chunkUploadRequest = HttpRequest.newBuilder()
                        .uri(URI.create(chunkServerUrl))
                        .header("chunkId", chunkMetaData.getChunkId())
                        .POST(HttpRequest.BodyPublishers.ofByteArray(chunkData))
                        .build();

                httpClient.send(chunkUploadRequest, HttpResponse.BodyHandlers.discarding());
            }
        }
    }

    public void downloadFile(String filename, String namespace) throws IOException, InterruptedException {
        Chunk chunk = new Chunk(filename, namespace, -1);
        HttpRequest chunkIdsRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://" + MASTER_SERVER_HOST + ":" + MASTER_SERVER_PORT + "/getChunkIds"))
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(chunk)))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> chunkIdsResponse = httpClient.send(chunkIdsRequest, HttpResponse.BodyHandlers.ofString());
        List<String> chunkIds = objectMapper.readValue(chunkIdsResponse.body(), new TypeReference<List<String>>() {
        });

        ByteArrayOutputStream fileOutputStream = new ByteArrayOutputStream();

        for (String chunkId : chunkIds) {
            List<ChunkServerInfo> chunkServers = getChunkServerInfo(chunkId);

            for (ChunkServerInfo chunkServerInfo : chunkServers) {
                String chunkServerUrl =
                        "http://" + chunkServerInfo.host + ":" + chunkServerInfo.port + "/readChunk?chunkId=" + chunkId;
                HttpRequest chunkDownloadRequest = HttpRequest.newBuilder()
                        .uri(URI.create(chunkServerUrl))
                        .GET()
                        .build();

                HttpResponse<byte[]> chunkDownloadResponse = httpClient.send(chunkDownloadRequest,
                        HttpResponse.BodyHandlers.ofByteArray());
                if (chunkDownloadResponse.statusCode() == 200) {
                    fileOutputStream.write(chunkDownloadResponse.body());
                    break;
                }
            }
        }

        File downloadDir = new File(DOWNLOAD_DIR);
        if (!downloadDir.exists()) {
            downloadDir.mkdir();
        }

        Files.write(Paths.get(DOWNLOAD_DIR, filename), fileOutputStream.toByteArray());
    }

    private List<ChunkServerInfo> getChunkServerInfo(String chunkId) throws IOException, InterruptedException {
        HttpRequest chunkServersRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://" + MASTER_SERVER_HOST + ":" + MASTER_SERVER_PORT + "/getChunkServers"))
                .POST(HttpRequest.BodyPublishers.ofString(chunkId))
                .header("Content-Type", "application/json")
                .build();

        HttpResponse<String> chunkInfoResponse = httpClient.send(chunkServersRequest,
                HttpResponse.BodyHandlers.ofString());
        return objectMapper.readValue(chunkInfoResponse.body(), new TypeReference<List<ChunkServerInfo>>() {
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Client client = new Client();
        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("Enter command (upload/download/exit): ");
            String command = scanner.nextLine()
                    .trim()
                    .toLowerCase();

            if ("exit".equals(command)) {
                break;
            }

            System.out.println("Enter filename: ");
            String filename = scanner.nextLine()
                    .trim();
            System.out.println("Enter namespace: ");
            String namespace = scanner.nextLine()
                    .trim();

            try {
                if ("upload".equals(command)) {
                    client.uploadFile(filename, namespace);
                } else if ("download".equals(command)) {
                    client.downloadFile(filename, namespace);
                } else {
                    System.out.println("Unknown command: " + command);
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("Operation failed: " + e.getMessage());
            }
        }
    }
}
