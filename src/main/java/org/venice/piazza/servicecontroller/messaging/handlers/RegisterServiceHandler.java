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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import model.job.PiazzaJobType;
import model.job.type.RegisterServiceJob;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.service.metadata.Service;

import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Handler for handling registerService requests.  This handler is used 
 * when register-service kafka topics are received or when clients utilize the 
 * ServiceController registerService web service.
 * @author mlynum
 * @version 1.0
 *
 */
@Component
public class RegisterServiceHandler implements PiazzaJobHandler {
	@Autowired
	private MongoAccessor mongoAccessor;
	@Autowired
	private ElasticSearchAccessor elasticAccessor;
	@Autowired
	private PiazzaLogger coreLogger;
	@Autowired
	private UUIDFactory uuidFactory;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RegisterServiceHandler.class);
	private static final long HTTP_REQUEST_TIMEOUT = 600;

	/**
	 * Handler for the RegisterServiceJob that was submitted. Stores the metadata in MongoDB
	 * 
	 * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
	 */
	@SuppressWarnings("deprecation")
	@Override
	public ResponseEntity<String> handle(PiazzaJobType jobRequest) {
		coreLogger.log("Registering a Service", PiazzaLogger.INFO);
		RegisterServiceJob job = (RegisterServiceJob) jobRequest;

		if (job != null) {
			// Get the Service metadata
			Service serviceMetadata = job.data;
			coreLogger.log("serviceMetadata received is " + serviceMetadata, PiazzaLogger.INFO);

			String result = handle(serviceMetadata);
			if (result.length() > 0) {
				String responseString = "{\"resourceId\":" + "\"" + result + "\"}";
				return new ResponseEntity<String>(responseString, HttpStatus.OK);
			} else {
				coreLogger.log("No result response from the handler, something went wrong", PiazzaLogger.ERROR);
				return new ResponseEntity<String>("RegisterServiceHandler handle didn't work", HttpStatus.UNPROCESSABLE_ENTITY);
			}
		} else {
			coreLogger.log("No RegisterServiceJob", PiazzaLogger.ERROR);
			return new ResponseEntity<String>("No RegisterServiceJob", HttpStatus.BAD_REQUEST);
		}
	}

	/**
	 * Handler for registering the new service with mongo and elastic search.
	 * 
	 * @param service
	 * @return resourceId of the registered service
	 */
	public String handle(Service service) {
		String resultServiceId = "";
		if (service != null) {
			resultServiceId = uuidFactory.getUUID();
			service.setServiceId(resultServiceId);
			
			// set default request timeout			
			if (null == service.getTimeout()) {
				service.setTimeout(HTTP_REQUEST_TIMEOUT);
			}

			resultServiceId = mongoAccessor.save(service);
			coreLogger.log("The result of the save is " + resultServiceId, PiazzaLogger.DEBUG);

			PiazzaResponse response = elasticAccessor.save(service);

			if (ErrorResponse.class.isInstance(response)) {
				ErrorResponse errResponse = (ErrorResponse) response;
				coreLogger.log("The result of the save is " + errResponse.message, PiazzaLogger.DEBUG);

			} else {
				coreLogger.log("Successfully stored service " + service.getServiceId(), PiazzaLogger.DEBUG);

			}
		} 
		
		return resultServiceId;
	}
}
