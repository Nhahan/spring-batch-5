package org.example.springbatch.batch.listener;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
public class JobListener implements JobExecutionListener {

    private static class TimeTracker {
        private Long startTime = null;
        private Long endTime = null;

        public void update(Long stepStartTime, Long stepEndTime) {
            if (startTime == null || stepStartTime < startTime) {
                startTime = stepStartTime;
            }
            if (endTime == null || stepEndTime > endTime) {
                endTime = stepEndTime;
            }
        }

        public Long getDuration() {
            if (startTime != null && endTime != null) {
                return endTime - startTime;
            }
            return null;
        }
    }

    @Override
    public void beforeJob(@NonNull JobExecution jobExecution) {
        log.info("Job 시작");
    }

    @Override
    public void afterJob(@NonNull JobExecution jobExecution) {
        log.info("Job 완료");

        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();

        Map<String, TimeTracker> timeTrackerMap = new HashMap<>();
        timeTrackerMap.put("순차 처리", new TimeTracker());
        timeTrackerMap.put("병렬 처리", new TimeTracker());

        for (StepExecution stepExecution : stepExecutions) {
            String stepName = stepExecution.getStepName();
            long stepStartTime = Objects.requireNonNull(stepExecution.getStartTime())
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            long stepEndTime = Objects.requireNonNull(stepExecution.getEndTime())
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

            String key = null;
            if (stepName.startsWith("sequentialStep")) {
                key = "순차 처리";
            }
            if (stepName.startsWith("parallelStep")) {
                key = "병렬 처리";
            }

            if (key != null) {
                timeTrackerMap.get(key).update(stepStartTime, stepEndTime);
            }
        }

        for (Map.Entry<String, TimeTracker> entry : timeTrackerMap.entrySet()) {
            Long duration = entry.getValue().getDuration();
            if (duration != null) {
                log.info(entry.getKey() + " 총 소요시간: " + duration + " ms");
            } else {
                log.info(entry.getKey() + " 시간 정보를 가져올 수 없습니다.");
            }
        }
    }
}
