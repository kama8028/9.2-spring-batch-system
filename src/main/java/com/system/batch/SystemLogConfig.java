package com.system.batch;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class SystemLogConfig {

  private final JobRepository jobRepository;
  private final PlatformTransactionManager transactionManager;

  public SystemLogConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    this.jobRepository = jobRepository;
    this.transactionManager = transactionManager;
  }

  @Bean
  public Job systemLogJob(Step systemLogStep) {
    return new JobBuilder("systemLogJob", jobRepository)
      .start(systemLogStep)
      .build();    
  }

  @Bean
  public Step systemLogStep(FlatFileItemReader<SystemLog> systemLogReader, ItemWriter<SystemLog> systemLogWriter) {
    return new StepBuilder("systemLogStep", jobRepository)
        .<SystemLog, SystemLog> chunk (10, transactionManager)
        .reader(systemLogReader)
        .writer(systemLogWriter)
        .build();
   }

   @Bean
   @StepScope
   public FlatFileItemReader<SystemLog> systemLogReader(
    @Value("#{jobParameters['inputFile']}") String inputFile) {
      return new FlatFileItemReaderBuilder<SystemLog>()
        .name("systemLogReader")
        .resource(new FileSystemResource(inputFile))
        .lineMapper(systemLogLineMapper())
        .build();
   }

   @Bean
   public PatternMatchingCompositeLineMapper<SystemLog> systemLogLineMapper() {
      PatternMatchingCompositeLineMapper<SystemLog> lineMapper =  new PatternMatchingCompositeLineMapper<>();

      Map<String, LineTokenizer> tokenizers = new HashMap<>();
      tokenizers.put("ERROR*", errorLineTokenizer());
      tokenizers.put("ABORT*", abortLineTokenizer());
      tokenizers.put("COLLECT*", collectLineTokenizer());
      lineMapper.setTokenizers(tokenizers);

      Map<String, FieldSetMapper<SystemLog>> mappers = new HashMap<>();
      mappers.put("ERROR*", new ErrorFiedSetMapper());
      mappers.put("ABORT*", new AbortFiedSetMapper());
      mappers.put("COLLECT*", new CollectFiedSetMapper());
      lineMapper.setFieldSetMappers(mappers);

      return lineMapper;      
   }

   @Bean
   public DelimitedLineTokenizer errorLineTokenizer() {
      DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
      tokenizer.setNames("type", "application", "timestamp", "message", "resouceUsage", "logPath");
      return tokenizer;
   }

   @Bean
    public DelimitedLineTokenizer abortLineTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("type", "application", "errorType", "timestamp", "message", "exitCode", "processPath", "status");
        return tokenizer;
    }

    @Bean
    public DelimitedLineTokenizer collectLineTokenizer() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("type", "dumpType", "processId", "timestamp", "dumpPath");
        return tokenizer;
    }

    @Bean
    public ItemWriter<SystemLog> systemLogWriter() {
      return items -> {
        for(SystemLog item : items) {
          log.info("{}", item);
        }
      };
    }  

   @Data
   public static class SystemLog {
      private String type;
      private String timestamp;
   }

   @Data
   @ToString(callSuper = true)
   
  
}
