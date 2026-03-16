package com.example.RocksdbEE.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

@Service
public class FileManagerService {

    @Value("${app.file.processed-dir}")
    private String processedDir;

    public void moveToProcessed(File file) {
        try {
            Path targetPath = Path.of(processedDir, file.getName());

            Files.createDirectories(Path.of(processedDir));

            Files.move(
                    file.toPath(),
                    targetPath,
                    StandardCopyOption.REPLACE_EXISTING
            );

            System.out.println("Moved to processed: " + file.getName());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}