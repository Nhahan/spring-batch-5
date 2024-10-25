package org.example.springbatch.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer {

    private final JdbcTemplate jdbcTemplate;

    @PostConstruct
    public void generateInitialData() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM CUSTOMER", Long.class);
        if (count == null || count == 0) {
            int numThreads = 10;
            int batchSize = 10000;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            log.info("데이터 생성 시작. 데이터 개수: " + numThreads * batchSize);
            for (int i = 0; i < numThreads; i++) {
                final int start = i * batchSize;
                executor.submit(() -> {
                    for (int j = start; j < start + batchSize; j++) {
                        jdbcTemplate.update("INSERT INTO CUSTOMER (NAME) VALUES (?)", "InitialName" + j);
                    }
                });
            }

            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    log.error("데이터 생성 실패");
                }
            } catch (InterruptedException e) {
                log.error("데이터 생성 실패", e);
            }
            log.info("초기 데이터 생성 완료");
        }
        count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM CUSTOMER", Long.class);
        log.info("데이터 개수: " + count);
    }
}
