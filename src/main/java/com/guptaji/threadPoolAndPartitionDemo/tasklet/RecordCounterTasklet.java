package com.guptaji.threadPoolAndPartitionDemo.tasklet;

import com.guptaji.threadPoolAndPartitionDemo.constant.Constant;
import com.guptaji.threadPoolAndPartitionDemo.repository.CustomerRepo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

public class RecordCounterTasklet implements Tasklet, StepExecutionListener {

  Logger LOG = LogManager.getLogger(RecordCounterTasklet.class);

  private StepExecution stepExecution;

  @Autowired private CustomerRepo customerRepo;

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
      throws Exception {
    long count = customerRepo.count();
    LOG.info("Count {}", count);
    ExecutionContext executionContext = this.stepExecution.getJobExecution().getExecutionContext();
    executionContext.putLong(Constant.COUNT, count);
    return null;
  }

  @Override
  public void beforeStep(StepExecution stepExecution) {
    this.stepExecution = stepExecution;
  }

  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    return null;
  }
}
