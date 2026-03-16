package com.example.RocksdbEE.scheduler;

import java.io.File;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.RocksdbEE.service.FileProcessingService;
import com.example.RocksdbEE.service.FileScannerService;


@Component
public class FileScheduler {
    
    private final FileScannerService fileScannerService;
    private final FileProcessingService fileProcessingService;

    @Value("${app.file.input-dir}")
    private String inputDir;

    public FileScheduler(FileScannerService fileScannerService, FileProcessingService fileProcessingService) {
        this.fileScannerService = fileScannerService;
        this.fileProcessingService = fileProcessingService;
    }

    @Scheduled(fixedDelayString = "${app.scheduler.fixed-delay}")
    public void scheduleFileProcessing() {
        List<File> files = fileScannerService.scan(inputDir);
        files.forEach(fileProcessingService::process);
    }

    @Scheduled(cron = "${app.scheduler.cleanup-cron}")
    public void scheduleCleanup() {
        System.out.println("Scheduler: Triggering cleanupOldData()...");
        fileProcessingService.cleanupOldData();
    }


}
