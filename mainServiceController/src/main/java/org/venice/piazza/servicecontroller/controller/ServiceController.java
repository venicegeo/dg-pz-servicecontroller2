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
package org.venice.piazza.servicecontroller.controller;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;
import org.springframework.validation.ObjectError;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.SearchServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;

import exception.DataInspectException;
import exception.InvalidInputException;
import model.data.DataType;
import model.job.type.RegisterServiceJob;
import model.logger.AuditElement;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.ServiceIdResponse;
import model.response.ServiceResponse;
import model.response.SuccessResponse;
import model.service.SearchCriteria;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.Service;
import util.PiazzaLogger;

/**
 * Purpose of this controller is to handle service requests for register in and managing services.
 * 
 * @author mlynum & Sonny.Saniev
 * @since 1.0
 */
@RestController
@RequestMapping({ "/servicecontroller", "" })
public class ServiceController {

	@Autowired
	private LocalValidatorFactoryBean validator;
	@Autowired
	private DeleteServiceHandler dlHandler;
	@Autowired
	private UpdateServiceHandler usHandler;
	@Autowired
	private DescribeServiceHandler dsHandler;
	@Autowired
	private ExecuteServiceHandler esHandler;
	@Autowired
	private ListServiceHandler lsHandler;
	@Autowired
	private MongoAccessor accessor;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private RegisterServiceHandler rsHandler;
	@Autowired
	private SearchServiceHandler ssHandler;

	private static final String DEFAULT_PAGE_SIZE = "10";
	private static final String DEFAULT_PAGE = "0";
	private final static Logger LOGGER = LoggerFactory.getLogger(ServiceController.class);

	/**
	 * Empty controller for now
	 */
	public ServiceController() {

	}

	/**
	 * Registers a service with the piazza service controller.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration and for testing of the
	 * serviceController.
	 * 
	 * @param serviceMetadata
	 *            metadata about the service
	 * @return A Json message with the resourceId {resourceId="<the id>"}
	 */
	@RequestMapping(value = "/registerService", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> registerService(@RequestBody PiazzaJobRequest jobRequest) {
		try {
			RegisterServiceJob serviceJob = (RegisterServiceJob) jobRequest.jobType;

			// For Task-Managed Services, URL is not required. For all other services, it is. Validate that here.
			if ((serviceJob.data.getIsTaskManaged() == null) || (serviceJob.data.getIsTaskManaged() == false)) {
				if ((serviceJob.data.getUrl() == null) || (serviceJob.data.getUrl().isEmpty())) {
					// Throw validation error
					throw new InvalidInputException("`url` property is required.");
				}
			}

			String serviceId = rsHandler.handle(serviceJob.data);
			return new ResponseEntity<PiazzaResponse>(new ServiceIdResponse(serviceId), HttpStatus.OK);
		} catch (Exception exception) {
			LOGGER.error("Error Registering Service", exception);
			logger.log(exception.toString(), Severity.ERROR);
			logger.log(String.format("Error Registering Service: %s", exception.getMessage()), Severity.ERROR,
					new AuditElement("serviceController", "registeringService", "jobRequest"));
			return new ResponseEntity<PiazzaResponse>(
					new ErrorResponse(String.format("Error Registering Service: %s", exception.getMessage()), "Service Controller"),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Gets service metadata, based on its Id.
	 * 
	 * @param serviceId
	 *            The Id of the service.
	 * @return The service metadata or appropriate error
	 */
	@RequestMapping(value = "/service/{serviceId}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getServiceInfo(@PathVariable(value = "serviceId") String serviceId) {
		try {
			// Check if Service exists
			try {
				return new ResponseEntity<PiazzaResponse>(new ServiceResponse(accessor.getServiceById(serviceId)), HttpStatus.OK);
			} catch (ResourceAccessException rae) {
				LOGGER.error("Service not found", rae);
				return new ResponseEntity<PiazzaResponse>(
						new ErrorResponse(String.format("Service not found: %s", serviceId), "Service Controller"), HttpStatus.NOT_FOUND);
			}
		} catch (Exception exception) {
			LOGGER.error("Could not look up Service", exception);
			logger.log(exception.toString(), Severity.ERROR);
			logger.log(String.format("Could not look up Service %s", exception.getMessage()), Severity.ERROR,
					new AuditElement("serviceController", "gettingServiceMetadata", "Service"));
			return new ResponseEntity<PiazzaResponse>(
					new ErrorResponse(String.format("Could not look up Service %s information: %s", serviceId, exception.getMessage()),
							"Service Controller"),
					HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Gets the list of services currently registered.
	 * 
	 * @return The list of registered services.
	 */
	@RequestMapping(value = "/service", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> getServices(
			@RequestParam(value = "page", required = false, defaultValue = DEFAULT_PAGE) Integer page,
			@RequestParam(value = "perPage", required = false, defaultValue = DEFAULT_PAGE_SIZE) Integer perPage,
			@RequestParam(value = "order", required = false, defaultValue = "asc") String order,
			@RequestParam(value = "sortBy", required = false, defaultValue = "serviceId") String sortBy,
			@RequestParam(value = "keyword", required = false) String keyword,
			@RequestParam(value = "userName", required = false) String userName) {
		try {
			// Don't allow for invalid orders
			if (!(order.equalsIgnoreCase("asc")) && !(order.equalsIgnoreCase("desc"))) {
				order = "asc";
			}
			return new ResponseEntity<PiazzaResponse>(accessor.getServices(page, perPage, order, sortBy, keyword, userName), HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error Listing Services: %s", exception.getMessage());
			LOGGER.error(error, exception);
			logger.log(error, Severity.ERROR);
			logger.log(error, Severity.ERROR,
					new AuditElement("serviceController", "gettingFullListOfRegisteredServices", "ServiceControllerMongoDB"));
			return new ResponseEntity<PiazzaResponse>(new ErrorResponse(error, "Service Controller"), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Deletes a registered service.
	 * 
	 * @param serviceId
	 *            The Id of the service to delete.
	 * @return Null if service is deleted without error, or error if an exception occurs..
	 */
	@RequestMapping(value = "/service/{serviceId}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> unregisterService(@PathVariable(value = "serviceId") String serviceId,
			@RequestParam(value = "softDelete", required = false) boolean softDelete) {
		try {
			// Check if Service exists
			try {
				accessor.getServiceById(serviceId);
			} catch (ResourceAccessException rae) {
				LOGGER.error("Service not found", rae);
				logger.log(String.format("Service not found %s", rae.getMessage()), Severity.ERROR,
						new AuditElement("serviceController", "deletingServiceMetadata", "Service"));
				return new ResponseEntity<PiazzaResponse>(
						new ErrorResponse(String.format("Service not found: %s", serviceId), "Service Controller"), HttpStatus.NOT_FOUND);
			}
			// remove from elastic search as well....
			dlHandler.handle(serviceId, softDelete);
			return new ResponseEntity<PiazzaResponse>(new SuccessResponse("Service was deleted successfully.", "ServiceController"),
					HttpStatus.OK);
		} catch (Exception exception) {
			String error = String.format("Error Deleting service %s: %s", serviceId, exception.getMessage());
			LOGGER.error(error, exception);
			logger.log(error, Severity.ERROR);
			logger.log(error, Severity.ERROR, new AuditElement("serviceController", "deletingServiceMetadata", "Service"));
			return new ResponseEntity<PiazzaResponse>(new ErrorResponse(error, "Service Controller"), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Updates a service with new Metadata.
	 * 
	 * @param serviceId
	 *            Service Id to delete.
	 * @param serviceData
	 *            The data of the service to update.
	 * @return Null if the service has been updated, or an appropriate error if there is one.
	 */
	@RequestMapping(value = "/service/{serviceId}", method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<PiazzaResponse> updateServiceMetadata(@PathVariable(value = "serviceId") String serviceId,
			@RequestBody Service serviceData) {
		try {
			// Ensure valid input
			if ((serviceId == null) || (serviceId.isEmpty())) {
				return new ResponseEntity<PiazzaResponse>(new ErrorResponse("The serviceId was not specified", "Service Controller"),
						HttpStatus.BAD_REQUEST);
			}

			// Get the existing service.
			Service existingService;
			try {
				existingService = accessor.getServiceById(serviceId);
			} catch (ResourceAccessException rae) {
				LOGGER.info(rae.getMessage(), rae);
				return new ResponseEntity<PiazzaResponse>(new ErrorResponse(rae.getMessage(), "ServiceController"), HttpStatus.NOT_FOUND);
			}

			// Log
			logger.log(String.format("Updating Service with ID %s", serviceId), Severity.INFORMATIONAL);

			// Merge the new defined properties into the existing service
			existingService.merge(serviceData, false);

			// Ensure the Service is still valid, with the new merged changes
			Errors errors = new BeanPropertyBindingResult(existingService, existingService.getClass().getName());
			validator.validate(existingService, errors);
			if (errors.hasErrors()) {
				// Build up the list of Errors
				StringBuilder builder = new StringBuilder();
				for (ObjectError error : errors.getAllErrors()) {
					builder.append(error.getDefaultMessage() + ".");
				}

				logger.log(String.format("Error validating updated Service Metadata. Validation Errors: %s", builder.toString()),
						Severity.ERROR, new AuditElement("serviceController", "updateServiceMetadata", "Service"));
				throw new DataInspectException(
						String.format("Error validating updated Service Metadata. Validation Errors: %s", builder.toString()));
			}

			// Update Existing Service in mongo
			existingService.setServiceId(serviceId);
			String result = usHandler.handle(existingService);
			if (result.length() > 0) {
				return new ResponseEntity<PiazzaResponse>(new SuccessResponse("Service was updated successfully.", "ServiceController"),
						HttpStatus.OK);
			} else {
				return new ResponseEntity<PiazzaResponse>(
						new ErrorResponse("The update for serviceId " + serviceId + " did not happen successfully", "ServiceController"),
						HttpStatus.INTERNAL_SERVER_ERROR);
			}

		} catch (Exception exception) {
			String error = String.format("Error Updating service %s: %s", serviceId, exception.getMessage());
			LOGGER.error(error, exception);
			logger.log(error, Severity.ERROR);
			logger.log(error, Severity.ERROR, new AuditElement("serviceController", "updateServiceMetadata", "Service"));
			return new ResponseEntity<PiazzaResponse>(new ErrorResponse(error, "ServiceController"), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * Executes a service registered in the Service Controller. This service is meant for internal Piazza use,
	 * Swiss-Army-Knife (SAK) administration and for testing of the serviceController.
	 * 
	 * @param data
	 *            ExecuteServiceData used to execute the data. Contains resourceId and values to use.
	 * 
	 * @return the results of the service execution
	 */
	@RequestMapping(value = "/executeService", method = RequestMethod.POST, headers = "Accept=application/json")
	public ResponseEntity<String> executeService(@RequestBody ExecuteServiceData data) {
		for (Map.Entry<String, DataType> entry : data.dataInputs.entrySet()) {
			String key = entry.getKey();
			logger.log("dataInput key:" + key, Severity.DEBUG);
			logger.log("dataInput Type:" + entry.getValue().getClass().getSimpleName(), Severity.DEBUG);
		}
		ResponseEntity<String> result = null;
		try {
			result = esHandler.handle(data);
		} catch (Exception ex) {
			LOGGER.error("Service Controller Error Caused Exception", ex);
			logger.log("Service Controller Error Caused Exception: " + ex.toString(), Severity.ERROR);
			logger.log(String.format("Service Controller Error Caused Exception: %s", ex.toString()), Severity.ERROR,
					new AuditElement("serviceController", "executingService", data.getServiceId()));
		}
		logger.log("Result is " + result, Severity.DEBUG);

		// Set the response based on the service retrieved
		return result;
	}

	/**
	 * Used to describe details about the service.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration and for testing of the
	 * serviceController.
	 * 
	 * @param resourceId
	 *            The id associated with the service that is registered within the Service Controller.
	 * @return Json with the ResourceMetadata, the metadata about the service
	 */
	@RequestMapping(value = "/describeService", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<String> describeService(@ModelAttribute("resourceId") String resourceId) {
		ResponseEntity<String> result = dsHandler.handle(resourceId);
		logger.log("Result is " + result, Severity.DEBUG);
		// Set the response based on the service retrieved
		return result;
	}

	/**
	 * deletes a registered service from the ServiceController.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration and for testing of the
	 * serviceController.
	 * 
	 * @param resourceId
	 * @return the result of the deletion
	 */
	@RequestMapping(value = "/deleteService", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<String> deleteService(@ModelAttribute("resourceId") String resourceId) {
		logger.log("deleteService resourceId=" + resourceId, Severity.INFORMATIONAL);
		String result = dlHandler.handle(resourceId, false);
		logger.log("Result is " + result, Severity.DEBUG);
		return new ResponseEntity<String>(result, HttpStatus.OK);
	}

	/**
	 * Lists all the services registered in the service controller.
	 * 
	 * This service is meant for internal Piazza use, Swiss-Army-Knife (SAK) administration and for testing of the
	 * serviceController.
	 * 
	 * @return Json list o resourceMetadata items (Metadata about the service)
	 */
	@RequestMapping(value = "/listService", method = RequestMethod.GET, headers = "Accept=application/json")
	public ResponseEntity<String> listService() {
		logger.log("listService", Severity.INFORMATIONAL);
		ResponseEntity<String> result = lsHandler.handle();
		logger.log("Result is " + result, Severity.DEBUG);
		return result;
	}

	/**
	 * Searches for registered services. This service is meant for internal Piazza use, Swiss-Army-Knife (SAK)
	 * administration and for testing of the serviceController.
	 * 
	 * @param SearchCriteria
	 *            The criteria to search with (specify field and regular expression
	 * 
	 * @return Json list o resourceMetadata items (Metadata about the service)
	 */
	@RequestMapping(value = "/search", method = RequestMethod.POST, headers = "Accept=application/json")
	public ResponseEntity<String> search(@RequestBody SearchCriteria criteria) {
		logger.log("search " + " " + criteria.field + "->" + criteria.pattern, Severity.INFORMATIONAL);
		ResponseEntity<String> result = ssHandler.handle(criteria);
		logger.log("Result is " + result, Severity.DEBUG);
		return result;
	}

	/**
	 * Healthcheck to see if the Piazza Service Controller is up and running. This service is meant for internal Piazza
	 * use, Swiss-Army-Knife (SAK) administration and for testing of the serviceController.
	 * 
	 * @return welcome message
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public ResponseEntity<String> healthCheck() {
		logger.log("Health Check called", Severity.DEBUG);
		HttpHeaders responseHeaders = new HttpHeaders();
		responseHeaders.setContentType(MediaType.valueOf("text/html"));
		String htmlMessage = "<HTML><TITLE>Piazza Service Controller Welcome</TITLE>";
		htmlMessage = htmlMessage + "<BODY><BR> Welcome from the Piazza Service Controller. "
				+ "<BR>For details on running and using the ServiceController, " + "<BR>see The Piazza Developer's Guide<A> for details."
				+ "<BODY></HTML>";
		ResponseEntity<String> response = new ResponseEntity<String>(htmlMessage, responseHeaders, HttpStatus.OK);

		return response;
	}

	/**
	 * Statistics for the Piazza Service controller This service is meant for internal Piazza use, Swiss-Army-Knife
	 * (SAK) administration and for testing of the serviceController.
	 * 
	 * @return json as statistics
	 */
	@RequestMapping(value = "/admin/stats", method = RequestMethod.GET)
	public void stats(HttpServletResponse response) throws IOException {
		response.sendRedirect("/metrics");
	}
}
