package com.qzg.batch.batchdemo.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

/**
 * @author 瞿兆刚
 * @date 2021/8/23
 */
@Slf4j
public class MyJobListener implements JobExecutionListener {


        @Override
        public void beforeJob(JobExecution jobExecution) {
            log.info("job 开始, id={}",jobExecution.getJobId());
        }

        @Override
        public void afterJob(JobExecution jobExecution) {
            log.info("job 结束, id={}",jobExecution.getJobId());
        }
    }

