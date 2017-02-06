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

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.async.AsynchronousServiceWorker;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.taskmanaged.ServiceTaskManager;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import exception.DataInspectException;
import exception.PiazzaJobException;
import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.Job;
import model.job.PiazzaJobType;
import model.job.metadata.ResourceMetadata;
import model.job.result.type.DataResult;
import model.job.result.type.ErrorResult;
import model.job.result.type.TextResult;
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import model.response.EventTypeListResponse;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import model.workflow.Event;
import model.workflow.EventType;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * 
 * @author mlynum & Sonny.Saniev
 *
 */
@Component
public class ServiceMessageWorker {

	@Value("${SPACE}")
	private String SPACE;

	@Value("${workflow.url}")
	private String WORKFLOW_URL;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private UUIDFactory uuidFactory;

	@Autowired
	private MongoAccessor accessor;

	@Autowired
	private PiazzaLogger coreLogger;

	@Autowired
	private ExecuteServiceHandler esHandler;

	@Autowired
	private AsynchronousServiceWorker asynchronousServiceWorker;

	@Autowired
	private ServiceTaskManager serviceTaskManager;

	@Autowired
	private RestTemplate restTemplate;

	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceMessageWorker.class);

	/**
	 * Handles service job requests on a thread
	 */
	@Async
	public Future<String> run(ConsumerRecord<String, String> consumerRecord, Producer<String, String> producer, Job job,
			WorkerCallback callback) {
		try {
			String executeJobStatus = StatusUpdate.STATUS_SUCCESS;
			String handleTextUpdate = "";
			ResponseEntity<String> externalServiceResponse = null;
			int statusCode = 400;

			// Ensure a valid Job has been received through Kafka
			if (job == null) {
				throw new DataInspectException("A Null Job has been received by the Service Controller Worker.");
			}

			// Ensure the Job Type is of Execute Service Job
			if ((job.getJobType() == null) || (job.getJobType() instanceof ExecuteServiceJob == false)) {
				throw new PiazzaJobException("An Invalid Job Type has been received by the Service Controller Worker.",
						HttpStatus.BAD_REQUEST.value());
			}

			// Process the Execution of the External Service
			try {
				PiazzaJobType jobType = job.getJobType();

				coreLogger.log("ExecuteServiceJob Detected with ID " + job.getJobId(), Severity.DEBUG);

				// Get the Job Data and Service information.
				ExecuteServiceJob jobItem = (ExecuteServiceJob) jobType;
				jobItem.setJobId(consumerRecord.key());
				ExecuteServiceData esData = jobItem.data;
				Service service = accessor.getServiceById(esData.getServiceId());

				// Send the Job Status that this Job has been handled. If this is a typical sync/async service, then the
				// request will be made directly to the Service URL. If this is a Task-Managed Service, then the Job
				// will be put into the Jobs queue.
				StatusUpdate su = new StatusUpdate();
				if ((service.getIsTaskManaged() != null) && (service.getIsTaskManaged().booleanValue())) {
					su.setStatus(StatusUpdate.STATUS_PENDING);
				} else {
					su.setStatus(StatusUpdate.STATUS_RUNNING);
				}
				ProducerRecord<String, String> statusUpdateRecord = new ProducerRecord<String, String>(
						String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), job.getJobId(),
						objectMapper.writeValueAsString(su));
				producer.send(statusUpdateRecord);

				if (esData.dataOutput != null) {

					if (Thread.interrupted()) {
						throw new InterruptedException();
					}
					// First check to see if the service is OFFLINE, if so
					// do not execute a thing
					ResourceMetadata rMetadata = service.getResourceMetadata();
					if ((rMetadata != null) && (rMetadata.getAvailability() != null)
							&& (rMetadata.getAvailability().equals(ResourceMetadata.STATUS_TYPE.OFFLINE.toString()))) {
						throw new DataInspectException(
								"The service " + esData.getServiceId() + " is " + ResourceMetadata.STATUS_TYPE.OFFLINE.toString());

					}
					// Determine if this is a Service that is processed Asynchronously, or is Task Managed. If so, then
					// branch here.
					if ((service.getIsAsynchronous() != null) && (service.getIsAsynchronous().booleanValue())) {
						// Perform Asynchronous Logic
						asynchronousServiceWorker.executeService(jobItem);
						// Return null. This future will not be tracked by the Service Thread Manager.
						// TODO: Once we can simplify/isolate some of the logic, I'd like to get to a spot where
						// we don't have to scatter return statements throughout this method.
						callback.onComplete(consumerRecord.key());
						return null;
					} else if ((service.getIsTaskManaged() != null) && (service.getIsTaskManaged().booleanValue())) {
						// If this is a Task Managed service, then insert this Job into the Task Management queue.
						serviceTaskManager.addJobToQueue(jobItem);
						callback.onComplete(consumerRecord.key());
						return null;
					}

					// If the Service is neither Asynchronous or Task Managed, then process it in the normal Synchronous
					// manner.

					coreLogger.log("ExecuteServiceJob Original Way", Severity.DEBUG);
					// Execute the external Service and get the Response Entity
					try {
						externalServiceResponse = esHandler.handle(jobType);
					} catch (Exception exception) {
						// InterruptedException to ensure a common handled exception type.
						LOGGER.info("Exception occurred", exception);
						throw new InterruptedException(exception.getMessage());
					}

					if (Thread.interrupted()) {
						throw new InterruptedException();
					}

					// If an internal error occurred during Service Handling, then throw an exception.
					if (externalServiceResponse.getStatusCode().is2xxSuccessful() == false) {
						throw new PiazzaJobException(
								String.format("Error %s with Status Code %s", externalServiceResponse.getBody(),
										externalServiceResponse.getStatusCode().toString()),
								externalServiceResponse.getStatusCode().value());
					}

					// Process the Response and handle any Ingest that may result
					String dataId = uuidFactory.getUUID();
					String outputType = jobItem.data.dataOutput.get(0).getClass().getSimpleName();

					DataResult result = esHandler.processExecutionResult(service, outputType, producer, executeJobStatus,
							externalServiceResponse, dataId);

					if (Thread.interrupted()) {
						throw new InterruptedException();
					}

					// If there is a Result, Send the Status Update with Result to the Job Manager component
					if (result != null) {
						StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
						statusUpdate.setResult(result);
						ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate,
								SPACE);
						producer.send(prodRecord);
					}

					// Fire Event to Workflow
					fireWorkflowEvent(job.getCreatedBy(), job.getJobId(), dataId, "Service completed successfully.");

					// Return.
					callback.onComplete(consumerRecord.key());
					return new AsyncResult<String>("ServiceMessageWorker_Thread");
				} else {
					externalServiceResponse = new ResponseEntity<>(
							"DataOuptut mimeType was not specified.  Please refer to the API for details.", HttpStatus.BAD_REQUEST);
				}
			} catch (IOException | ResourceAccessException ex) {
				LOGGER.error("Exception occurred", ex);
				coreLogger.log(ex.getMessage(), Severity.ERROR);
				executeJobStatus = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = ex.getMessage();
			} catch (HttpClientErrorException | HttpServerErrorException hex) {
				LOGGER.error("HttpException occurred", hex);
				coreLogger.log(hex.getMessage(), Severity.ERROR);
				executeJobStatus = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = hex.getResponseBodyAsString();
				statusCode = hex.getStatusCode().value();
			} catch (PiazzaJobException pex) {
				LOGGER.error("PiazzaJobException occurred", pex);
				coreLogger.log(pex.getMessage(), Severity.ERROR);
				executeJobStatus = StatusUpdate.STATUS_ERROR;
				handleTextUpdate = pex.getMessage();
				statusCode = pex.getStatusCode();
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// If there was no result set then use the default error messages set.
			if (externalServiceResponse == null) {
				sendErrorStatus(executeJobStatus, handleTextUpdate, statusCode, producer, job.getJobId());
			} else {
				// If the status is not OK and the job is not null
				// then send an update to the job manager that there was some failure
				boolean eResult = (externalServiceResponse.getStatusCode() != HttpStatus.OK) ? true : false;
				if (eResult) {
					sendErrorStatus(StatusUpdate.STATUS_FAIL, externalServiceResponse,
							new Integer(externalServiceResponse.getStatusCode().value()), producer, job.getJobId());
				}
			}
		} catch (InterruptedException ex) { // NOSONAR normal handling of InterruptedException
			coreLogger.log(String.format("Thread for Job %s was interrupted.", job.getJobId()), Severity.INFORMATIONAL);
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_CANCELLED);
			TextResult result = new TextResult(ex.toString());
			statusUpdate.setResult(result);

			try {
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, SPACE));
			} catch (JsonProcessingException jsonException) {
				LOGGER.error("Json processing error occurred", jsonException);
				coreLogger.log(String.format(
						"Error sending Cancelled Status from Job %s: %s. The Job was cancelled, but its status will not be updated in the Job Manager.",
						consumerRecord.key(), jsonException.getMessage()), Severity.ERROR);
			}
		} catch (Exception ex) {
			LOGGER.error("Unexpected Error in processing External Service", ex);
			// Catch any General Exceptions that occur during runtime.
			coreLogger.log(ex.getMessage(), Severity.ERROR);
			sendErrorStatus(StatusUpdate.STATUS_ERROR, "Unexpected Error in processing External Service: " + ex.getMessage(),
					HttpStatus.INTERNAL_SERVER_ERROR.value(), producer, job.getJobId());
		}

		// Return Future
		callback.onComplete(consumerRecord.key());
		return new AsyncResult<String>("ServiceMessageWorker_Thread");
	}

	/**
	 * Sends an error Status to the Job Manager.
	 * 
	 * @param status
	 *            The status. Corresponds with StatusUpdate constants.
	 * @param message
	 *            The message, detailing the error.
	 * @param statusCode
	 *            The numeric HTTP status code.
	 */
	private void sendErrorStatus(String status, Object message, Integer statusCode, Producer<String, String> producer, String jobId) {
		StatusUpdate statusUpdate = new StatusUpdate();
		statusUpdate.setStatus(status);
		// Create a text result and update status
		ErrorResult errorResult = new ErrorResult();
		errorResult.setMessage(message);
		errorResult.setStatusCode(statusCode);
		statusUpdate.setResult(errorResult);

		try {
			ProducerRecord<String, String> prodRecord = new ProducerRecord<String, String>(
					String.format("%s-%s", JobMessageFactory.UPDATE_JOB_TOPIC_NAME, SPACE), jobId,
					objectMapper.writeValueAsString(statusUpdate));
			producer.send(prodRecord);
		} catch (JsonProcessingException exception) {
			// The message could not be serialized. Record this.
			LOGGER.error("Json processing error occurred", exception);
			coreLogger.log("Could not send Error Status to Job Manager. Error serializing Status: " + exception.getMessage(),
					Severity.ERROR);
		}
	}

	/**
	 * Fires the event to the Workflow service that a Service has completed execution.
	 */
	private void fireWorkflowEvent(String user, String jobId, String dataId, String message) {
		coreLogger.log("Firing Event for Completion of ExecuteServiceJob execution", Severity.DEBUG);
		try {
			// Retrieve piazza:executionCompletion EventTypeId from pz-workflow.
			String url = String.format("%s/%s?name=%s", WORKFLOW_URL, "eventType", "piazza:executionComplete");
			EventType eventType = objectMapper.readValue(restTemplate.getForObject(url, String.class), EventTypeListResponse.class).data
					.get(0);

			// Construct Event object
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("jobId", jobId);
			data.put("status", message);
			data.put("dataId", dataId);

			Event event = new Event();
			event.createdBy = user;
			event.eventTypeId = eventType.eventTypeId;
			event.data = data;

			// Call pz-workflow endpoint to fire Event object
			restTemplate.postForObject(String.format("%s/%s", WORKFLOW_URL, "event"), objectMapper.writeValueAsString(event), String.class);
		} catch (HttpClientErrorException | HttpServerErrorException exception) {
			String error = String.format("Could not successfully send Event to Workflow Service. Returned with code %s and message %s",
					exception.getStatusCode().toString(), exception.getResponseBodyAsString());
			coreLogger.log(error, Severity.ERROR);
			LOGGER.error(error, exception);
		} catch (IOException exception) {
			String error = String.format("Could not send Event to Workflow Service. Serialization of Event failed with Error: %s",
					exception.getMessage());
			LOGGER.error(error, exception);
			coreLogger.log(error, Severity.ERROR);
		}
	}

	/**
	 * This method is for demonstrating ingest of raster data This will be refactored once the API changes have been
	 * communicated to other team members
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public void handleRasterType(ExecuteServiceJob executeJob, Job job, Producer<String, String> producer)
			throws InterruptedException, JsonParseException, JsonMappingException, IOException {
		RestTemplate restTemplate = new RestTemplate();
		ExecuteServiceData data = executeJob.data;
		// Get the id from the data
		String serviceId = data.getServiceId();
		Service sMetadata = accessor.getServiceById(serviceId);
		// Default request mimeType application/json
		String requestMimeType = "application/json";

		UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl());
		Map<String, DataType> postObjects = new HashMap<String, DataType>();
		Iterator<Entry<String, DataType>> it = data.getDataInputs().entrySet().iterator();
		String postString = "";

		while (it.hasNext()) {
			Entry<String, DataType> entry = it.next();
			String inputName = entry.getKey();

			if (entry.getValue() instanceof URLParameterDataType) {
				String paramValue = ((TextDataType) entry.getValue()).getContent();
				if (inputName.length() == 0) {
					builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl() + "?" + paramValue);
				} else {
					builder.queryParam(inputName, paramValue);
				}
			} else if (entry.getValue() instanceof BodyDataType) {
				BodyDataType bdt = (BodyDataType) entry.getValue();
				postString = bdt.getContent();
				requestMimeType = bdt.getMimeType();
			}
			// Default behavior for other inputs, put them in list of objects
			// which are transformed into JSON consistent with default
			// requestMimeType
			else {
				postObjects.put(inputName, entry.getValue());
			}
		}
		if (postString.length() > 0 && postObjects.size() > 0) {
			return;
		} else if (postObjects.size() > 0) {
			try {
				postString = objectMapper.writeValueAsString(postObjects);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				LOGGER.error("Json processing error occurred", e);
			}
		}
		URI url = URI.create(builder.toUriString());
		HttpHeaders headers = new HttpHeaders();

		// Set the mimeType of the request
		MediaType mediaType = createMediaType(requestMimeType);
		headers.setContentType(mediaType);
		// Set the mimeType of the request
		// headers.add("Content-type",
		// sMetadata.getOutputs().get(0).getDataType().getMimeType());

		if (postString.length() > 0) {

			coreLogger.log("The postString is " + postString, Severity.DEBUG);

			HttpHeaders theHeaders = new HttpHeaders();
			// headers.add("Authorization", "Basic " + credentials);
			theHeaders.setContentType(MediaType.APPLICATION_JSON);

			// Create the Request template and execute
			HttpEntity<String> request = new HttpEntity<String>(postString, theHeaders);

			coreLogger.log("About to call special service " + url, Severity.DEBUG);

			if (null != sMetadata.getTimeout()) {
				HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
				factory.setReadTimeout(sMetadata.getTimeout().intValue() * 1000);
				factory.setConnectTimeout(sMetadata.getTimeout().intValue() * 1000);
				restTemplate = new RestTemplate(factory);
			}
			ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			coreLogger.log("The Response is " + response.getBody(), Severity.DEBUG);

			String serviceControlString = response.getBody();
			coreLogger.log("Service Control String " + serviceControlString, Severity.DEBUG);

			DataResource dataResource = objectMapper.readValue(serviceControlString, DataResource.class);
			coreLogger.log("dataResource type is " + dataResource.getDataType().getClass().getSimpleName(), Severity.DEBUG);

			dataResource.dataId = uuidFactory.getUUID();
			coreLogger.log("dataId " + dataResource.dataId, Severity.DEBUG);

			PiazzaJobRequest pjr = new PiazzaJobRequest();
			pjr.createdBy = "pz-sc-ingest-raster-test";

			IngestJob ingestJob = new IngestJob();
			ingestJob.data = dataResource;
			ingestJob.host = true;
			pjr.jobType = ingestJob;

			ProducerRecord<String, String> newProdRecord = JobMessageFactory.getRequestJobMessage(pjr, uuidFactory.getUUID(), SPACE);
			producer.send(newProdRecord);

			coreLogger.log("newProdRecord sent " + newProdRecord.toString(), Severity.DEBUG);

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			fireWorkflowEvent(job.getCreatedBy(), job.getJobId(), dataResource.dataId, "Service completed successfully.");

			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);

			// Create a text result and update status
			DataResult textResult = new DataResult(dataResource.dataId);
			statusUpdate.setResult(textResult);
			ProducerRecord<String, String> prodRecord = JobMessageFactory.getUpdateStatusMessage(job.getJobId(), statusUpdate, SPACE);

			producer.send(prodRecord);
			coreLogger.log("prodRecord sent " + prodRecord.toString(), Severity.DEBUG);
		}
	}

	public MediaType createMediaType(String mimeType) {
		MediaType mediaType;
		String type, subtype;
		StringBuffer sb = new StringBuffer(mimeType);
		int index = sb.indexOf("/");
		// If a slash was found then there is a type and subtype
		if (index != -1) {
			type = sb.substring(0, index);

			subtype = sb.substring(index + 1, mimeType.length());
			mediaType = new MediaType(type, subtype);
			coreLogger.log("The type is=" + type, Severity.DEBUG);
			coreLogger.log("The subtype is=" + subtype, Severity.DEBUG);

		} else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);
		}

		return mediaType;
	}
}