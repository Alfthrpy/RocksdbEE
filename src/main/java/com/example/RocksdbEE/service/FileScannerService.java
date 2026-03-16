package com.example.RocksdbEE.service;

import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Service
public class FileScannerService {

    public List<File> scan(String directoryPath) {
        System.out.println("Scanning directory: " + directoryPath);
        File folder = new File(directoryPath);
        List<File> files = new ArrayList<>();

        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("Directory not found: " + directoryPath);
            return files;
        }

        for (File file : folder.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".csv")) {
                files.add(file);
            }
        }

        System.out.println("Found " + files.size() + " file(s) in: " + directoryPath);
        return files;
    }
}