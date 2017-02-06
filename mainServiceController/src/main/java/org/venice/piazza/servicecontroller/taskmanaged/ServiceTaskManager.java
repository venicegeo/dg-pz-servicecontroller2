/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.venice.piazza.servicecontroller.taskmanaged;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;

import exception.InvalidInputException;
import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.job.Job;
import model.job.result.type.ErrorResult;
import model.job.type.ExecuteServiceJob;
import model.logger.AuditElement;
import model.logger.Severity;
import model.service.metadata.Service;
import model.service.taskmanaged.ServiceJob;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * Functionality for Task-Managed Services.
 * <p>
 * Task-Managed Services use Piazza Service Controller as a way to manage their job tasking/queueing. In the case of a
 * Task-Managed Service, Piazza makes no calls externally to that registered Service. Instead, that registered Service
 * is given a Jobs Queue in Piazza, that is populated with Service Execution Jobs that come in through Piazza. That
 * external Service is then responsible for pulling Jobs off of its Jobs queue in order to do the work.
 * </p>
 * <p>
 * This functionality allows for external registered Services to have an easy, 80% solution for task/queueing of Jobs in
 * an entirely RESTful manner, where Piazza handles the persistence and messaging of these Jobs.
 * </p>
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class ServiceTaskManager {
	@Value("${SPACE}")
	private String SPACE;
	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_HOSTS;
	@Value("${task.managed.error.limit}")
	private Integer TIMEOUT_LIMIT_COUNT;

	@Autowired
	private ObjectMapper objectMapper;
	@Autowired
	private MongoAccessor mongoAccessor;
	@Autowired
	private PiazzaLogger piazzaLogger;

	private Producer<String, String> producer;
	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceTaskManager.class);

	@PostConstruct
	public void initialize() {
		producer = KafkaClientFactory.getProducer(KAFKA_HOSTS);
	}

	/**
	 * Creates a Service Queue for a newly registered Job.
	 * 
	 * @param serviceId
	 *            The Id of the Service
	 */
	public void createServiceQueue(String serviceId) {
		// TODO: Initialization can be done here.
	}

	/**
	 * Adds a Job to the Service's queue.
	 * 
	 * @param job
	 *            The Job to be executed. This information contains the serviceId, which is used to lookup the
	 *            appropriate Service Queue.
	 */
	public void addJobToQueue(ExecuteServiceJob job) {
		// Add the Job to the Jobs queue
		ServiceJob serviceJob = new ServiceJob(job.getJobId(), job.getData().getServiceId());
		mongoAccessor.addJobToServiceQueue(job.getData().getServiceId(), serviceJob);
		// Update the Job Status as Pending to Kafka
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(StatusUpdate.STATUS_PENDING);
		ProducerRecord<String, String> statusUpdateRecord;
		try {
			statusUpdateRecord = new ProducerRecord<String, String>(String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE),
					job.getJobId(), objectMapper.writeValueAsString(statusUpdate));
			producer.send(statusUpdateRecord);
		} catch (JsonProcessingException exception) {
			String error = "Error Sending Pending Job Status to Job Manager: " + exception.getMessage();
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Attempts to cancel a Job. This will use a Job lookup in order to find the Service that was executed. If this
	 * Service was task managed, then that Job will be removed from the queue.
	 * 
	 * @param jobId
	 *            The ID of the Job to attempt to cancel.
	 */
	public void cancelJob(String jobId) {
		try {
			// Attempt to get the Service that executed this Job
			Job job = mongoAccessor.getJobById(jobId);
			if (job != null) {
				// If this was an Execute Service Job
				if (job.getJobType() instanceof ExecuteServiceJob) {
					ExecuteServiceJob executeJob = (ExecuteServiceJob) job.getJobType();
					String serviceId = executeJob.getData().getServiceId();

					// Log the cancellation
					piazzaLogger.log(String.format("Removing Service Job %s from Service Queue for %s", jobId, serviceId),
							Severity.INFORMATIONAL);

					// Determine if the Service ID is Task-Managed
					Service service = mongoAccessor.getServiceById(serviceId);
					if (service.getIsTaskManaged() == true) {
						// If this is a Task Managed Service, then remove the Job from the Queue.
						mongoAccessor.removeJobFromServiceQueue(serviceId, jobId);
						// Send the Kafka Message that this Job has been cancelled
						StatusUpdate statusUpdate = new StatusUpdate();
						statusUpdate.setStatus(StatusUpdate.STATUS_CANCELLED);
						ProducerRecord<String, String> statusUpdateRecord;
						try {
							statusUpdateRecord = new ProducerRecord<String, String>(
									String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), jobId,
									objectMapper.writeValueAsString(statusUpdate));
							producer.send(statusUpdateRecord);
						} catch (JsonProcessingException exception) {
							String error = String.format("Error Sending Cancelled Job %s Status to Job Manager: %s", jobId,
									exception.getMessage());
							LOGGER.error(error, exception);
							piazzaLogger.log(error, Severity.ERROR);
						}

						// Log the success
						piazzaLogger.log(String.format("Successfully removed Service Job %s from Service Queue for %s", jobId, serviceId),
								Severity.INFORMATIONAL);
					}
				}
			}
		} catch (Exception exception) {
			String error = String.format("Error Removing Job %s from a Task-Managed Service Queue : %s", jobId, exception.getMessage());
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Processes the external Worker requesting a Status Update for a running job.
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param jobId
	 *            The ID of the Job
	 * @param statusUpdate
	 *            The Status of the Job
	 */
	public void processStatusUpdate(String serviceId, String jobId, StatusUpdate statusUpdate)
			throws MongoException, InvalidInputException {
		// Validate the Service ID exists, and contains the Job ID
		ServiceJob serviceJob = mongoAccessor.getServiceJob(serviceId, jobId);
		if (serviceJob == null) {
			throw new InvalidInputException(String.format("Cannot find the specified Job %s for this Service %s", jobId, serviceId));
		}
		// Send the Update to Kafka
		ProducerRecord<String, String> statusUpdateRecord;
		try {
			statusUpdateRecord = new ProducerRecord<String, String>(String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE),
					jobId, objectMapper.writeValueAsString(statusUpdate));
			producer.send(statusUpdateRecord);
		} catch (JsonProcessingException exception) {
			String error = "Error Sending Job Status from External Service to Job Manager: " + exception.getMessage();
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR);
		}
		// If done, remove the Job from the Service Queue
		String status = statusUpdate.getStatus();
		if ((StatusUpdate.STATUS_CANCELLED.equals(status)) || (StatusUpdate.STATUS_ERROR.equals(status))
				|| (StatusUpdate.STATUS_FAIL.equals(status)) || (StatusUpdate.STATUS_SUCCESS.equals(status))) {
			piazzaLogger.log(String.format("Job %s For Service %s has reached final state %s. Removing from Service Jobs Queue.", jobId,
					serviceId, status), Severity.INFORMATIONAL);
			mongoAccessor.removeJobFromServiceQueue(serviceId, jobId);
		}
	}

	/**
	 * Pulls the next waiting Job off of the Jobs queue and returns it.
	 * 
	 * @param serviceId
	 *            The ID of the Service whose Queue to pull a Job from
	 * @return The Job information
	 */
	public ExecuteServiceJob getNextJobFromQueue(String serviceId)
			throws ResourceAccessException, InterruptedException, InvalidInputException {
		// Pull the Job off of the queue.
		ServiceJob serviceJob = mongoAccessor.getNextJobInServiceQueue(serviceId);

		// If no Job exists in the Queue, then return null. No work needs to be done.
		if (serviceJob == null) {
			return null;
		}

		// Read the Jobs collection for the full Job Details
		String jobId = serviceJob.getJobId();
		Job job = mongoAccessor.getJobById(jobId);
		// Ensure the Job exists. If it does not, then throw an error.
		if (job == null) {
			String error = String.format(
					"Error pulling Service Job off Job Queue for Service %s and Job Id %s. The Job was not found in the database.",
					serviceId, jobId);
			piazzaLogger.log(error, Severity.ERROR);
			throw new ResourceAccessException(error);
		}

		// Update the Job Status as Running to Kafka
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(StatusUpdate.STATUS_RUNNING);
		ProducerRecord<String, String> statusUpdateRecord;
		try {
			statusUpdateRecord = new ProducerRecord<String, String>(String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE),
					jobId, objectMapper.writeValueAsString(statusUpdate));
			producer.send(statusUpdateRecord);
		} catch (JsonProcessingException exception) {
			String error = "Error Sending Pending Job Status to Job Manager: ";
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR);
		}

		// Return the Job Execution Information, including payload and parameters.
		if (job.getJobType() instanceof ExecuteServiceJob) {
			// Ensure that the ServiceJob has the JobID populated
			ExecuteServiceJob executeServiceJob = (ExecuteServiceJob) job.getJobType();
			if (executeServiceJob.getJobId() == null) {
				executeServiceJob.setJobId(jobId);
			}
			// Return
			return executeServiceJob;
		} else {
			// The Job must be an ExecuteServiceJob. If for some reason it is not, then throw an error.
			String error = String.format(
					"Error pulling Job %s off of the Jobs Queue for Service %s. The Job was not the proper ExecuteServiceJob type. This Job cannot be processed.",
					jobId, serviceId);
			piazzaLogger.log(error, Severity.ERROR);
			throw new InvalidInputException(error);
		}
	}

	/**
	 * Handles the Timeout logic for a timed out Service Job.
	 * 
	 * @param serviceId
	 *            The Service ID
	 * @param serviceJob
	 *            The ServiceJob that has timed out
	 */
	public void processTimedOutServiceJob(String serviceId, ServiceJob serviceJob) {
		// Check if the Job has received too many timeouts thus far.
		if (serviceJob.getTimeouts().intValue() >= TIMEOUT_LIMIT_COUNT) {
			String error = String.format(
					"Service Job %s for Service %s has timed out too many times and is being removed from the Jobs Queue.", serviceId,
					serviceJob.getJobId());
			piazzaLogger.log(error, Severity.INFORMATIONAL,
					new AuditElement("serviceController", "failTimedOutJob", serviceJob.getJobId()));
			// If the Job has too many timeouts, then fail the Job.
			mongoAccessor.removeJobFromServiceQueue(serviceId, serviceJob.getJobId());
			// Send the Kafka message that this Job has failed.
			StatusUpdate statusUpdate = new StatusUpdate();
			statusUpdate.setResult(new ErrorResult("Service Timed Out", error));
			statusUpdate.setStatus(StatusUpdate.STATUS_ERROR);
			ProducerRecord<String, String> statusUpdateRecord;
			try {
				statusUpdateRecord = new ProducerRecord<String, String>(
						String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), serviceJob.getJobId(),
						objectMapper.writeValueAsString(statusUpdate));
				producer.send(statusUpdateRecord);
			} catch (JsonProcessingException exception) {
				String innerError = "Error Sending Failed/Timed Out Job Status to Job Manager: ";
				LOGGER.error(innerError, exception);
				piazzaLogger.log(innerError, Severity.ERROR);
			}
		} else {
			// Otherwise, increment the failure count and try again.
			piazzaLogger.log(String.format("Service Job %s for Service %s has timed out for the %s time and will be retried again.",
					serviceId, serviceJob.getJobId(), serviceJob.getTimeouts() + 1), Severity.INFORMATIONAL);
			// Increment the failure count and tag for retry
			mongoAccessor.incrementServiceJobTimeout(serviceId, serviceJob);
		}

	}
}
