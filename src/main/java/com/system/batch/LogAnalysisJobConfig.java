package com.system.batch;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class LogAnalysisJobConfig {
  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;


}
