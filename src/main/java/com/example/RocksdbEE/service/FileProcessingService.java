package com.example.RocksdbEE.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class FileProcessingService {

    private final RocksDBService rocksDBService;
    private final FileManagerService fileManagerService;

    @Value("${app.rocksdb.use-batch:false}")
    private boolean useBatch;

    @Value("${app.rocksdb.batch-size:1000}")
    private int batchSize;

    public FileProcessingService(RocksDBService rocksDBService, FileManagerService fileManagerService) {
        this.rocksDBService = rocksDBService;
        this.fileManagerService = fileManagerService;
    }

    public void process(File file) {

        String fileName = file.getName();
        boolean shouldMove = false;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {

            String status = rocksDBService.getMetadata(fileName);
            String fileId = UUID.randomUUID().toString();

            if (status != null && (status.startsWith("COMPLETED") || status.startsWith("ARCHIVED"))) {
                System.out.println("File " + fileName + " already processed. Skipping...");
                shouldMove = true;
            } else {
                rocksDBService.saveMetadata(fileName, "PROCESSING", fileId);

                if (useBatch) {
                    processWithBatch(br, fileId);
                } else {
                    processSingle(br, fileId);
                }

                rocksDBService.saveMetadata(fileName, "COMPLETED", fileId);
                shouldMove = true;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (shouldMove) {
            fileManagerService.moveToProcessed(file);
        }
    }

    private void processSingle(BufferedReader br, String fileId) throws Exception {
        String line;
        int lineNum = 0;
        while ((line = br.readLine()) != null) {
            rocksDBService.save(fileId + ":" + (lineNum++), line);
        }
    }

    private void processWithBatch(BufferedReader br, String fileId) throws Exception {
        Map<String, String> buffer = new HashMap<>();
        String line;
        int lineNum = 0;

        while ((line = br.readLine()) != null) {
            buffer.put(fileId + ":" + (lineNum++), line);

            if (buffer.size() >= batchSize) {
                rocksDBService.saveInBatch(buffer);
                buffer.clear();
            }
        }

        if (!buffer.isEmpty()) {
            rocksDBService.saveInBatch(buffer);
        }
    }

    public void cleanupOldData() {
        try {
            rocksDBService.cleanupOldData(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}