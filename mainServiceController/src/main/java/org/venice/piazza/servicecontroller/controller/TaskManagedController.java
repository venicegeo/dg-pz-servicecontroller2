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
package org.venice.piazza.servicecontroller.controller;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.taskmanaged.ServiceTaskManager;

import exception.InvalidInputException;
import model.job.type.ExecuteServiceJob;
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.ServiceJobResponse;
import model.response.SuccessResponse;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * REST Controller for Task-Managed Service endpoints. This includes pulling Jobs off the queue, and updating Status for
 * jobs. Also metrics such as queue length are available.
 * 
 * @author Patrick.Doody
 *
 */
@RestController
public class TaskManagedController {
	@Autowired
	private PiazzaLogger piazzaLogger;
	@Autowired
	private ServiceTaskManager serviceTaskManager;
	@Autowired
	private MongoAccessor mongoAccessor;

	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceController.class);

	/**
	 * Pulls the next job off of the Service Queue.
	 * 
	 * @param userName
	 *            The name of the user. Used for verification.
	 * @param serviceId
	 *            The ID of the Service
	 * @return The information for the next Job, if one is present.
	 */
	@RequestMapping(value = { "/service/{serviceId}/task" }, method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getNextServiceJobFromQueue(@RequestParam(value = "userName", required = true) String userName,
			@PathVariable(value = "serviceId") String serviceId) {
		try {
			// Log the Request
			piazzaLogger.log(String.format("User %s Requesting to perform Work on Next Job for %s Service Queue.", userName, serviceId),
					Severity.INFORMATIONAL);

			// Check for Access
			boolean canAccess = mongoAccessor.canUserAccessServiceQueue(serviceId, userName);
			if (!canAccess) {
				throw new ResourceAccessException("Service does not allow this user to access.");
			}

			// Get the Job. This will mark the Job as being processed.
			ExecuteServiceJob serviceJob = serviceTaskManager.getNextJobFromQueue(serviceId);
			// Return
			if (serviceJob != null) {
				// Return Job Information
				return new ResponseEntity<>(new ServiceJobResponse(serviceJob, serviceJob.getJobId()), HttpStatus.OK);
			} else {
				// No Job Found. Return Null in the Response.
				return new ResponseEntity<>(new ServiceJobResponse(), HttpStatus.OK);
			}

		} catch (Exception exception) {
			String error = String.format("Error Getting next Service Job for Service %s by User %s: %s", serviceId, userName,
					exception.getMessage());
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR, new AuditElement(userName, "errorGettingServiceJob", serviceId));
			HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
			if (exception instanceof ResourceAccessException) {
				status = HttpStatus.UNAUTHORIZED;
			} else if (exception instanceof InvalidInputException) {
				status = HttpStatus.NOT_FOUND;
			}
			return new ResponseEntity<>(new ErrorResponse(error, "ServiceController"), status);
		}
	}

	/**
	 * Updates the Status for a Piazza Job.
	 * 
	 * @param userName
	 *            The name of the user. Used for verification.
	 * @param serviceId
	 *            The ID of the Service containing the Job
	 * @param jobId
	 *            The ID of the Job to update
	 * @param statusUpdate
	 *            The update contents, including status, percentage, and possibly results.
	 * @return Success or error.
	 */
	@RequestMapping(value = {
			"/service/{serviceId}/task/{jobId}" }, method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> updateServiceJobStatus(@RequestParam(value = "userName", required = true) String userName,
			@PathVariable(value = "serviceId") String serviceId, @PathVariable(value = "jobId") String jobId,
			@RequestBody StatusUpdate statusUpdate) {
		try {
			// Log the Request
			piazzaLogger.log(String.format("User %s Requesting to Update Job Status for Job %s for Task-Managed Service.", userName, jobId),
					Severity.INFORMATIONAL);

			// Check for Access
			boolean canAccess = mongoAccessor.canUserAccessServiceQueue(serviceId, userName);
			if (!canAccess) {
				throw new ResourceAccessException("Service does not allow this user to access.");
			}

			// Simple Validation
			if ((statusUpdate.getStatus() == null) || (statusUpdate.getStatus().isEmpty())) {
				throw new HttpServerErrorException(HttpStatus.BAD_REQUEST, "`status` property must be provided in Update payload.");
			}

			// Process the Update
			serviceTaskManager.processStatusUpdate(serviceId, jobId, statusUpdate);
			// Return Success
			return new ResponseEntity<>(new SuccessResponse("OK", "ServiceController"), HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Could not Update status for Job %s for Service %s : %s", jobId, serviceId,
					exception.getMessage());
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR, new AuditElement(userName, "failedToUpdateServiceJob", jobId));
			HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
			if (exception instanceof ResourceAccessException) {
				status = HttpStatus.UNAUTHORIZED;
			} else if (exception instanceof InvalidInputException) {
				status = HttpStatus.NOT_FOUND;
			} else if (exception instanceof HttpServerErrorException) {
				status = ((HttpServerErrorException) exception).getStatusCode();
			}
			return new ResponseEntity<>(new ErrorResponse(error, "ServiceController"), status);
		}
	}

	/**
	 * Gets metadata for a specific Task-Managed Service.
	 * 
	 * @param userName
	 *            The name of the user. Used for verification.
	 * @param serviceId
	 *            The ID of the Service
	 * @return Map containing information regarding the Task-Managed Service
	 */
	@RequestMapping(value = {
			"/service/{serviceId}/task/metadata" }, method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<?> getServiceQueueData(@RequestParam(value = "userName", required = true) String userName,
			@PathVariable(value = "serviceId") String serviceId) {
		try {
			// Log the Request
			piazzaLogger.log(String.format("User %s Requesting Task-Managed Service Information for Service %s", userName, serviceId),
					Severity.INFORMATIONAL);

			// Check for Access
			boolean canAccess = mongoAccessor.canUserAccessServiceQueue(serviceId, userName);
			if (!canAccess) {
				throw new ResourceAccessException("Service does not allow this user to access.");
			}

			// Ensure this Service exists and is Task-Managed
			Service service = mongoAccessor.getServiceById(serviceId);
			if ((service.getIsTaskManaged() == null) || (service.getIsTaskManaged() == false)) {
				throw new InvalidInputException("The specified Service is not a Task-Managed Service.");
			}
			// Fill Map with Metadata
			Map<String, Object> response = mongoAccessor.getServiceQueueCollectionMetadata(serviceId);
			// Respond
			return new ResponseEntity<Map<String, Object>>(response, HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Could not retrieve Service Queue data for %s : %s", serviceId, exception.getMessage());
			LOGGER.error(error, exception);
			piazzaLogger.log(error, Severity.ERROR, new AuditElement(userName, "failedToRetrieveServiceQueueMetadata", serviceId));
			HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
			if (exception instanceof ResourceAccessException) {
				status = HttpStatus.UNAUTHORIZED;
			} else if (exception instanceof InvalidInputException) {
				status = HttpStatus.NOT_FOUND;
			}
			return new ResponseEntity<String>(error, status);
		}
	}
}
