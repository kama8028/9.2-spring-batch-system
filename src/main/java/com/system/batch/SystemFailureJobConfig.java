package com.system.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.core.step.builder.StepBuilder;

import lombok.Data;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class SystemFailureJobConfig {

  @Autowired
  private JobRepository jobRepository;

  @Autowired
  private PlatformTransactionManager transactionManager;

  @Bean
  public Job systemFailureJob(Step systemFailureStep) {
    return new JobBuilder("systemFailureJob", jobRepository)
      .start(systemFailureStep)
      .build();
  }

  @Bean
  public Step systemFailureStep(FlatFileItemReader<SystemFailure> systemFailureItemReader, SystemFailureStdoutItemWriter  systemFailureStdoutItemWriter) {
    return new StepBuilder("systemFailureStep", jobRepository)
      //FlatFileItemReader는 파일의 한 줄을 읽어 이를 SystemFailure로 변환하는 작업을 청크당 10번 반복 한다.
      .<SystemFailure, SystemFailure>chunk(10, transactionManager)
      .reader(systemFailureItemReader)
      .writer(systemFailureStdoutItemWriter)
      .build();
  }

  /*
   * [SYSTEM] Configuration Analysis:
     [SYSTEM] .name() : Reader 식별자 설정
     [SYSTEM] .resource() : 처리 대상 파일 지정
     [SYSTEM] .delimited() : 구분자 기반 파일 읽기 모드 활성화
     [SYSTEM] .delimiter(",") : 쉼표로 데이터 구분
     [SYSTEM] .names() : 각 필드 식별자(SystemFailure의 프로퍼티 이름) 매핑
     [SYSTEM] .targetType() : 변환 대상 객체(SystemFailure) 지정
     [SYSTEM] .linesToSkip() : 헤더 라인 제거
     [SYSTEM] .strict(): 엄격한 규율 적용
  */
  @Bean
  @StepScope
  public FlatFileItemReader<SystemFailure> systemFailureItemReader(@Value("#{jobParameters['inputFile']}") String inputFile){
    return new FlatFileItemReaderBuilder<SystemFailure>()
      .name("systemFailureItemReader")
      //읽어 드릴 Resource를 지정한다. 입력 파일의 경우(inpuFile)는 잡 파라미터로부터 동적으로 전달받고 있다.
      .resource(new FileSystemResource(inputFile))
      /*FlatFileReader에게 읽어들일 파일이 구분자로 분리된 형식임을 알리는 설정으로 가장 핵심이 되는 설정이다. 
       * DefaultLieMapper가 사용한 LineTokenizer 구현체로 DelimitedLineTokenizer가 지정된다. 앞서 설명했듯이 DelimitedLineTokenizer는 구분자로 구분된 데이터를 토큰화 한다. 
      */
      .delimited()
      .delimiter(",")
      .names("errorId","errorDateTime","severity","processId","errorMessage")
      .targetType(SystemFailure.class)
      /*한줄 건너 뛰고 두번째 줄부터 실제 데이터를 처리 한다. */
      .linesToSkip(1)
      /*파일과 데이터 검증의 강도를 설정하는 메서드로 기본값은 true이다. 이 경우 파일 누락시 예외를 발생시켜 배치를 중단하고 false면 파일이 존재하지 않다고 경고만 남기고 진행한다. */
      .strict(true)
      .build();
  }

  @Bean
  public SystemFailureStdoutItemWriter systemFailureStdoutItemWriter() {
    return new SystemFailureStdoutItemWriter();
  }

  public static class SystemFailureStdoutItemWriter implements ItemWriter<SystemFailure> {
    
    @Override
    public void write(Chunk<? extends SystemFailure> chunk) throws Exception {
      for(SystemFailure failure : chunk) {
        log.info("Processing system failure : {}", failure);
      }
    }
  }

  @Data
  public static class SystemFailure {
    private String errorId;
    private String errorDateTime;
    private String severity;
    private Integer processId;
    private String errorMessage;
  }
  
}
