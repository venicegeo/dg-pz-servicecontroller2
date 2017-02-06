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
package org.venice.piazza.servicecontroller.messaging;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.async.AsyncServiceInstanceScheduler;
import org.venice.piazza.servicecontroller.taskmanaged.ServiceTaskManager;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import messaging.job.WorkerCallback;
import model.job.Job;
import model.job.type.AbortJob;
import model.job.type.ExecuteServiceJob;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import util.PiazzaLogger;

@Component
public class ServiceMessageThreadManager {

	private String EXECUTE_SERVICE_JOB_TOPIC_NAME;

	/*
	 * TODO need to determine how statuses will be sent to update the job (Call back?)
	 */
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private Map<String, Future<?>> runningServiceRequests;
	private List<String> topics;
	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String KAFKA_HOSTS;
	@Value("${kafka.group.name}")
	private String KAFKA_GROUP;
	private final AtomicBoolean closed;
	@Value("${SPACE}")
	private String SPACE;

	@Autowired
	private CoreServiceProperties coreServiceProperties;
	@Autowired
	private PiazzaLogger coreLogger;
	@Autowired
	ServiceMessageWorker serviceMessageWorker;
	@Autowired
	private ServiceTaskManager serviceTaskManager;
	@Autowired
	private AsyncServiceInstanceScheduler asyncServiceInstanceManager;

	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceMessageThreadManager.class);

	/**
	 * Constructor for ServiceMessageThreadManager
	 */
	public ServiceMessageThreadManager() {
		closed = makeAtomicBoolean();

	}

	/**
	 * Initializing stuff
	 */
	@PostConstruct
	public void initialize() {

		// Initialize dynamic topic names
		EXECUTE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", (new ExecuteServiceJob()).getClass().getSimpleName(), SPACE);

		topics = Arrays.asList(EXECUTE_SERVICE_JOB_TOPIC_NAME);

		coreLogger.log("============================================================", Severity.INFORMATIONAL);
		coreLogger.log("EXECUTE_SERVICE_JOB_TOPIC_NAME=" + EXECUTE_SERVICE_JOB_TOPIC_NAME, Severity.INFORMATIONAL);
		coreLogger.log("KAFKA_GROUP=" + KAFKA_GROUP, Severity.INFORMATIONAL);
		coreLogger.log("KAFKA_HOSTS=" + KAFKA_HOSTS, Severity.INFORMATIONAL);
		coreLogger.log("============================================================", Severity.INFORMATIONAL);

		/* Initialize producer and consumer for the Kafka Queue */
		producer = KafkaClientFactory.getProducer(KAFKA_HOSTS);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOSTS, KAFKA_GROUP);

		// Initialize the HashMap
		runningServiceRequests = new HashMap<String, Future<?>>();

		// Start polling for Kafka Jobs on the Group Consumer.. This occurs on a
		// separate Thread so as not to block Spring.
		Thread kafkaListenerThread = new Thread() {
			@Override
			public void run() {
				pollServiceJobs();
			}
		};

		// Subscribe for the topics
		consumer.subscribe(topics);
		kafkaListenerThread.start();

		// Start polling for Kafka Abort Jobs on the unique Consumer.
		Thread pollAbortThread = new Thread() {
			@Override
			public void run() {
				pollAbortServiceJobs();
			}
		};
		pollAbortThread.start();
	}

	/**
	 * Polls for service controller topics and handles these requests.
	 */
	public void pollServiceJobs() {

		ObjectMapper mapper = makeObjectMapper();
		try {
			Job job;

			// Create a Callback that will be invoked when a Worker completes.
			// This will
			WorkerCallback callback = new WorkerCallback() {
				@Override
				public void onComplete(String jobId) {
					runningServiceRequests.remove(jobId);
				}
			};
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					try {
						job = mapper.readValue(consumerRecord.value(), Job.class);

						if (job != null) {
							// Log the request.
							coreLogger.log(String.format("Received Job Request to process Topic %s with Job Id %s", consumerRecord.topic(),
									consumerRecord.key()), Severity.INFORMATIONAL);

							// start a new thread
							Future<?> workerFuture = serviceMessageWorker.run(consumerRecord, producer, job, callback);

							runningServiceRequests.put(consumerRecord.key(), workerFuture);
						}

					} catch (Exception ex) {
						coreLogger.log(String.format("The item received did not marshal to a job: %s", ex), Severity.CRITICAL);
					}
				} // for loop
			} // while loop
		} catch (Exception ex) {
			coreLogger.log(String.format("The item received did not marshal to a job: %s", ex), Severity.CRITICAL);

		}

	}

	/**
	 * Begins listening for Abort Jobs. If a Job is owned by this component, then it will be terminated.
	 */
	public void pollAbortServiceJobs() {
		Consumer<String, String> uniqueConsumer = KafkaClientFactory.getConsumer(KAFKA_HOSTS,
				String.format("%s-%s", KAFKA_GROUP, UUID.randomUUID().toString()));
		ObjectMapper mapper = new ObjectMapper();

		try {
			// Create the Unique Consumer

			uniqueConsumer.subscribe(Arrays.asList(String.format("%s-%s", JobMessageFactory.ABORT_JOB_TOPIC_NAME, SPACE)));

			// Poll
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = uniqueConsumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					// Determine if this Job Id is being processed by this
					// component.
					String jobId = null;
					try {
						PiazzaJobRequest request = mapper.readValue(consumerRecord.value(), PiazzaJobRequest.class);
						jobId = ((AbortJob) request.jobType).getJobId();
					} catch (Exception exception) {
						String error = String.format("Error Aborting Job. Could not get the Job ID from the Kafka Message with error:  %s",
								exception.getMessage());
						LOGGER.error(error, exception);
						coreLogger.log(error, Severity.ERROR);
						continue;
					}

					// Determine if this a Sync or Async job
					if (runningServiceRequests.containsKey(jobId)) {
						// Cancel the Running Synchronous Job by terminating its thread
						boolean cancelled = runningServiceRequests.get(jobId).cancel(true);
						if (cancelled) {
							// Log the cancellation has occurred
							coreLogger.log(String.format("Successfully requested termination of Job thread for Job ID %s", jobId),
									Severity.INFORMATIONAL);
						} else {
							coreLogger.log(String.format(
									"Attempted to Cancel running job thread for ID %s, but the thread could not be forcefully cancelled.",
									jobId), Severity.ERROR);
						}
						// Remove it from the list of Running Jobs
						runningServiceRequests.remove(jobId);
					} else {
						// Is this a Task Managed Job? Remove it from the Jobs queue if it is pending.
						serviceTaskManager.cancelJob(jobId);
						// Is this an Async Job? Send a cancellation to the running service.
						asyncServiceInstanceManager.cancelInstance(jobId);
					}

				}
			}
			uniqueConsumer.close();
		} catch (WakeupException wex) {
			LOGGER.error("Polling Thread forcefully closed", wex);
			coreLogger.log(String.format("Polling Thread forcefully closed: %s", wex.getMessage()), Severity.CRITICAL);
			uniqueConsumer.close();
		} catch (Exception ex) {
			LOGGER.error("Polling Thread forcefully closed", ex);
			coreLogger.log(String.format("Polling Thread forcefully closed: %s", ex.getMessage()), Severity.CRITICAL);
			uniqueConsumer.close();
		}
	}

	public ObjectMapper makeObjectMapper() {
		return new ObjectMapper();
	}

	public AtomicBoolean makeAtomicBoolean() {
		return new AtomicBoolean();
	}
}
