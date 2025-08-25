package com.system.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.RegexLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class LogAnalysisJobConfig {
  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;

  @Bean
  public Job logAnalysisJob(Step logAnalysisStep) {
    return new JobBuilder("logAnalysisJob", jobRepository)
      .start(logAnalysisStep)
      .build();    
  }

  @Bean
  public Step logAnalysisStep(FlatFileItemReader<LogEntry> logItemReader, ItemWrite<LogEntry> logItemWirte)
  {
    return new StepBuilder("logAnalysisStep", jobRepository)
        .<LogEntry, LogEntry> chunk (10, transactionManager)
        .reader(logItemReader)
        .wirter(logItemWriter)
        .builder();      
  }

  @Bean 
  @StepScope
  public FlatFileItemReader<LogEntry> logItemReader(@Value("#{jobParameters['inputFile']}") String inputFile)) {
    RegexLineTokenizer tokenizer =  new RegexLineTokenizer();
    tokenizer.setRegex("\\[\\w+\\]\\[Thread-(\\d+)\\]\\CPU: \\D+%](.+)");

    return new FlatFileItemReader<LogEntry>()
      .name("logItemReader")
      .resource(new FileSystemResource(inputFile))
      .lineToekenizer(tokenizer)
      .fieldSetMapper(fieldSet -> new LogEntry(filedSet.readString(0), fieldSet.readString(1)))
      .build();
  }

  @Bean
  public ItemWriter<LogEntry> logItemWrite() {
    return items -> {
      for(logEntry logEntry : items) {
        log.info(String.format("THD-%$s: %s", logEntry.getThreadNum(), logEntry.getMessage()));
      }
    };
  }

  @Data
  @AllArgsConstructor
  public static class LogEntry {
       private String threadNum;
       private String message;
  }

}
