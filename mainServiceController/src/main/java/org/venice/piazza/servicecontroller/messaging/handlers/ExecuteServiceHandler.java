/*******************************************************************************
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
 *******************************************************************************/
package org.venice.piazza.servicecontroller.messaging.handlers;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.GeoJsonDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.PiazzaJobType;
import model.job.result.type.DataResult;
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.logger.AuditElement;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Handler for handling executeService requests. This handler is used when execute-service kafka topics are received or
 * when clients utilize the ServiceController service.
 * 
 * @author mlynum & Sonny.Saniev
 * @version 1.0
 */
@Component
public class ExecuteServiceHandler implements PiazzaJobHandler {

	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PiazzaLogger coreLogger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private RestTemplate template;
	@Autowired
	private ObjectMapper objectMapper;

	@Value("${SPACE}")
	private String SPACE;

	private final static Logger LOGGER = LoggerFactory.getLogger(ExecuteServiceHandler.class);

	/**
	 * Handler for handling execute service requests. This method will execute a service given the resourceId and return
	 * a response to the job manager. (non-Javadoc)
	 * 
	 * @throws InterruptedException
	 * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
	 */
	@Override
	public ResponseEntity<String> handle(PiazzaJobType jobRequest) throws InterruptedException {
		coreLogger.log("Executing a Service.", Severity.DEBUG);

		ExecuteServiceJob job = (ExecuteServiceJob) jobRequest;
		// Check to see if this is a valid request
		if (job != null) {
			// Get the ResourceMetadata
			ExecuteServiceData esData = job.data;
			ResponseEntity<String> handleResult = handle(esData);
			ResponseEntity<String> result = new ResponseEntity<>(handleResult.getBody(), handleResult.getStatusCode());
			coreLogger.log("The result is " + result, Severity.DEBUG);

			// TODO Use the result, send a message with the resource Id and jobId
			return result;
		} else {
			coreLogger.log("Job is null", Severity.ERROR);
			return new ResponseEntity<>("Job is null", HttpStatus.BAD_REQUEST);
		}
	}

	/**
	 * Handles requests to execute a service. TODO this needs to change to leverage pz-jbcommon ExecuteServiceMessage
	 * after it builds.
	 * 
	 * @param message
	 * @return the Response as a String
	 * @throws InterruptedException
	 */
	public ResponseEntity<String> handle(ExecuteServiceData data) throws InterruptedException {
		coreLogger.log(String.format("Beginning execution of Service ID %s", data.getServiceId()), Severity.INFORMATIONAL);
		ResponseEntity<String> responseEntity = null;
		String serviceId = data.getServiceId();
		Service sMetadata = null;
		// Default request mimeType application/json
		String requestMimeType = "application/json";
		try {
			// Accessor throws exception if can't find service
			sMetadata = accessor.getServiceById(serviceId);

			ObjectMapper om = new ObjectMapper();
			String result = om.writeValueAsString(sMetadata);
			coreLogger.log(result, Severity.INFORMATIONAL);
		} catch (ResourceAccessException | JsonProcessingException ex) {
			LOGGER.error("Exception occurred", ex);
		}
		if (sMetadata != null) {
			String rawURL = sMetadata.getUrl();
			coreLogger.log(String.format("Executing Service with URL %s with ID %s", rawURL, serviceId), Severity.INFORMATIONAL);
			UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(rawURL);

			Map<String, DataType> postObjects = new HashMap<>();
			Iterator<Entry<String, DataType>> it = data.getDataInputs().entrySet().iterator();
			String postString = "";
			while (it.hasNext()) {
				Entry<String, DataType> entry = it.next();
				String inputName = entry.getKey();
				coreLogger.log("The parameter is " + inputName, Severity.DEBUG);

				if (entry.getValue() instanceof URLParameterDataType) {
					String paramValue = ((URLParameterDataType) entry.getValue()).getContent();
					if (inputName.length() == 0) {
						coreLogger.log("sMetadata.getResourceMeta=" + sMetadata.getResourceMetadata(), Severity.DEBUG);
						builder = UriComponentsBuilder.fromHttpUrl(sMetadata.getUrl() + "?" + paramValue);
						coreLogger.log("Builder URL is " + builder.toUriString(), Severity.DEBUG);
					} else {
						builder.queryParam(inputName, paramValue);
						coreLogger.log("Input Name=" + inputName + " paramValue=" + paramValue, Severity.DEBUG);
					}
				} else if (entry.getValue() instanceof BodyDataType) {
					BodyDataType bdt = (BodyDataType) entry.getValue();
					postString = bdt.getContent();
					requestMimeType = bdt.getMimeType();
					if ((requestMimeType == null) || (requestMimeType.length() == 0)) {
						coreLogger.log("Body mime type not specified", Severity.ERROR);
						return new ResponseEntity<>("Body mime type not specified", HttpStatus.BAD_REQUEST);
					}
				} else {
					// Default behavior for other inputs, put them in list of objects
					// which are transformed into JSON consistent with default requestMimeType
					coreLogger.log("inputName =" + inputName + "entry Value=" + entry.getValue(), Severity.INFORMATIONAL);
					postObjects.put(inputName, entry.getValue());
				}
			}

			coreLogger.log("Final Builder URL" + builder.toUriString(), Severity.INFORMATIONAL);
			if (postString.length() > 0 && postObjects.size() > 0) {
				coreLogger.log("String Input not consistent with other Inputs", Severity.ERROR);
				return new ResponseEntity<>("String Input not consistent with other Inputs", HttpStatus.BAD_REQUEST);
			} else if (postObjects.size() > 0) {
				ObjectMapper mapper = makeObjectMapper();
				try {
					postString = mapper.writeValueAsString(postObjects);
				} catch (JsonProcessingException e) {
					LOGGER.error("Json processing error occurred", e);
					coreLogger.log(e.getMessage(), Severity.ERROR);
					return new ResponseEntity<>("Could not marshal post requests", HttpStatus.BAD_REQUEST);
				}
			}

			URI url = URI.create(builder.toUriString());
			if (sMetadata.getMethod().equals("GET")) {
				coreLogger.log("GetForEntity URL=" + url, Severity.INFORMATIONAL);
				// execute job
				responseEntity = template.getForEntity(url, String.class);
			} else if (sMetadata.getMethod().equals("POST")) {
				HttpHeaders headers = new HttpHeaders();
				// Set the mimeType of the request
				MediaType mediaType = createMediaType(requestMimeType);
				headers.setContentType(mediaType);
				HttpEntity<String> requestEntity = makeHttpEntity(headers, postString);

				coreLogger.log("PostForEntity URL=" + url, Severity.INFORMATIONAL);
				try {
					responseEntity = template.postForEntity(url, requestEntity, String.class);
				} catch (HttpServerErrorException hex) {
					LOGGER.info(String.format("Server Error for URL %s:  %s", url, hex.getResponseBodyAsString()), hex);
					throw new InterruptedException(hex.getResponseBodyAsString());
				}
			} else {
				coreLogger.log("Request method type not specified", Severity.ERROR);
				return new ResponseEntity<>("Request method type not specified", HttpStatus.BAD_REQUEST);
			}

			coreLogger.log(String.format("Triggered execution of service %s", sMetadata.getServiceId()), Severity.INFORMATIONAL,
					new AuditElement("serviceController", "executingExternalService", sMetadata.getServiceId()));
		} else {
			coreLogger.log(String.format("The service was NOT found id %s", data.getServiceId()), Severity.ERROR,
					new AuditElement("serviceController", "notFound", data.getServiceId()));
			return new ResponseEntity<>("Service Id " + data.getServiceId() + " not found", HttpStatus.NOT_FOUND);
		}
		return responseEntity;
	}

	/**
	 * This method creates a MediaType based on the mimetype that was provided
	 * 
	 * @param mimeType
	 * @return MediaType
	 */
	private MediaType createMediaType(String mimeType) {
		MediaType mediaType;
		String type, subtype;
		StringBuffer sb = new StringBuffer(mimeType);
		int index = sb.indexOf("/");
		// If a slash was found then there is a type and subtype
		if (index != -1) {
			type = sb.substring(0, index);

			subtype = sb.substring(index + 1, mimeType.length());
			mediaType = new MediaType(type, subtype);
		} else {
			// Assume there is just a type for the mime, no subtype
			mediaType = new MediaType(mimeType);
		}

		return mediaType;
	}

	/**
	 * create HttpEntity
	 */
	public HttpEntity<String> makeHttpEntity(HttpHeaders headers, String postString) {
		HttpEntity<String> requestEntity;

		if (postString.length() > 0)
			requestEntity = new HttpEntity<>(postString, headers);
		else
			requestEntity = new HttpEntity<>(headers);

		return requestEntity;

	}

	ObjectMapper makeObjectMapper() {
		return new ObjectMapper();
	}

	/**
	 * Processes the Result of the external Service execution. This will send the Ingest job through Kafka, and will
	 * return the Result of the data.
	 */
	public DataResult processExecutionResult(Service service, String outputType, Producer<String, String> producer, String status,
			ResponseEntity<String> handleResult, String dataId) throws JsonProcessingException, IOException, InterruptedException {
		coreLogger.log("Send Execute Status Kafka", Severity.DEBUG);
		// Initialize ingest job items
		DataResource data = new DataResource();
		PiazzaJobRequest jobRequest = new PiazzaJobRequest();
		IngestJob ingestJob = new IngestJob();

		if (handleResult != null) {
			coreLogger.log("The result provided from service is " + handleResult.getBody(), Severity.DEBUG);

			// String serviceControlString = handleResult.getBody().get(0).toString();
			String serviceControlString = handleResult.getBody().toString();

			coreLogger.log("The service controller string is " + serviceControlString, Severity.DEBUG);

			try {
				// Now produce a new record
				jobRequest.createdBy = service.getResourceMetadata().getCreatedBy();
				data.dataId = dataId;
				coreLogger.log("dataId is " + data.dataId, Severity.DEBUG);
				data = objectMapper.readValue(serviceControlString, DataResource.class);

				// Now check to see if the conversion is actually a proper DataResource
				// if it is not time to create a TextDataType and return
				if ((data == null) || (data.getDataType() == null)) {
					coreLogger.log("The DataResource is not in a valid format, creating a new DataResource and TextDataType",
							Severity.DEBUG);

					data = new DataResource();
					data.dataId = dataId;
					TextDataType tr = new TextDataType();
					tr.content = serviceControlString;
					coreLogger.log("The data being sent is " + tr.content, Severity.DEBUG);

					data.dataType = tr;
				} else if ((null != data) && (null != data.getDataType())) { // NOSONAR
					data.dataId = dataId;
				}
			} catch (Exception ex) {
				LOGGER.error("Exception occurred", ex);
				coreLogger.log(ex.getMessage(), Severity.ERROR);

				// Checking payload type and settings the correct type
				if (outputType.equals((new TextDataType()).getClass().getSimpleName())) { // NOSONAR
					TextDataType newDataType = new TextDataType();
					newDataType.content = serviceControlString;
					data.dataType = newDataType;
				} else if (outputType.equals((new GeoJsonDataType()).getClass().getSimpleName())) { // NOSONAR
					GeoJsonDataType newDataType = new GeoJsonDataType();
					newDataType.setGeoJsonContent(serviceControlString);
					data.dataType = newDataType;
				}
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			ingestJob.data = data;
			ingestJob.host = true;
			jobRequest.jobType = ingestJob;

			String jobId = uuidFactory.getUUID();
			ProducerRecord<String, String> newProdRecord = JobMessageFactory.getRequestJobMessage(jobRequest, jobId, SPACE);
			producer.send(newProdRecord);

			coreLogger.log(String.format("Sending Ingest Job Id %s for Data Id %s for Data of Type %s", jobId, data.getDataId(),
					data.getDataType().getClass().getSimpleName()), Severity.INFORMATIONAL);

			// Return the Result of the Data.
			DataResult textResult = new DataResult(data.dataId);
			return textResult;
		}
		return null;
	}
}