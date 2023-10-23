package com.guptaji.threadPoolAndPartitionDemo.controller;

import java.time.LocalDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

  Logger LOG = LogManager.getLogger(JobController.class);

  @Autowired private JobLauncher jobLauncher;

  @Autowired private Job job;

  @PostMapping("/DbToDb")
  public void importCsvToDb()
      throws JobInstanceAlreadyCompleteException,
          JobExecutionAlreadyRunningException,
          JobParametersInvalidException,
          JobRestartException {
    JobParameters parameters =
        new JobParametersBuilder()
            .addLocalDateTime("startAt", LocalDateTime.now())
            .toJobParameters();

    JobExecution run = jobLauncher.run(job, parameters);

    LOG.info("Job status {}", run.getStatus());
  }
}
