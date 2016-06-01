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


import java.util.ArrayList;
import java.util.List;

import model.job.PiazzaJobType;
import model.job.type.DeleteServiceJob;
import model.service.metadata.Service;
import util.PiazzaLogger;
import util.UUIDFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.elasticsearch.accessors.ElasticSearchAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Handler for handling registerService requests.  This handler is used 
 * when register-service kafka topics are received or when clients utilize the 
 * ServiceController registerService web service.
 * @author mlynum
 * @version 1.0
 *
 */

public class DeleteServiceHandler implements PiazzaJobHandler {
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private UUIDFactory uuidFactory;
	private ElasticSearchAccessor elasticAccessor;
	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteServiceHandler.class);

	public DeleteServiceHandler(MongoAccessor accessor, ElasticSearchAccessor elasticAccessor, CoreServiceProperties coreServiceProp, PiazzaLogger coreLogger,
			UUIDFactory uuidFactory)
	{
		this.accessor = accessor;
		this.coreLogger = coreLogger;
		this.uuidFactory = uuidFactory;
		this.elasticAccessor = elasticAccessor;
	}

	/**
	 * Handler for the RegisterServiceJob that was submitted. Stores the
	 * metadata in MongoDB (non-Javadoc)
	 * 
	 * @see
	 * org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(
	 * model.job.PiazzaJobType)
	 */
	public ResponseEntity<String> handle(PiazzaJobType jobRequest) {

		LOGGER.debug("Updating a service");
		DeleteServiceJob job = (DeleteServiceJob) jobRequest;
		if (job != null) {
			// Get the ResourceMetadata
			String resourceId = job.serviceID;
			LOGGER.info("describeService serviceId=" + resourceId);
			coreLogger.log("describeService serviceId=" + resourceId, coreLogger.INFO);

			String result = handle(resourceId, false);
			if (result.length() > 0) {
				String jobId = job.getJobId();
				ArrayList<String> resultList = new ArrayList<String>();
				resultList.add(jobId);
				resultList.add(resourceId);
				return new ResponseEntity<String>(resultList.toString(), HttpStatus.OK);

			} else {
				LOGGER.error("No result response from the handler, something went wrong");
				coreLogger.log("No result response from the handler, something went wrong", coreLogger.ERROR);
				return new ResponseEntity<String>("DeleteServiceHandler handle didn't work", HttpStatus.NOT_FOUND);
			}
		} else {
			return null;
		}
	}// handle

	/**
	 * Deletes resource by removing from mongo, and sends delete request to elastic search
	 * 
	 * @param rMetadata
	 * @return resourceID of the registered service
	 */
	public String handle(String resourceId, boolean softDelete) {
		coreLogger.log("about to delete a registered service.", PiazzaLogger.INFO);
		LOGGER.info("about to delete a registered service.");

		Service service = accessor.getServiceById(resourceId);
		elasticAccessor.delete(service);

		ObjectMapper mapper = new ObjectMapper();
		String result = "";
		try {
			result = mapper.writeValueAsString(accessor.delete(resourceId, softDelete));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		LOGGER.debug("The result of the delete is " + result);

		if (result.length() > 0) {
			coreLogger.log("The service with id " + resourceId + " was deleted " + result, PiazzaLogger.INFO);
			LOGGER.info("The service with id " + resourceId + " was deleted " + result);
		} else {
			coreLogger.log("The service with id " + resourceId + " was NOT deleted", PiazzaLogger.INFO);
			LOGGER.info("The service with id " + resourceId + " was NOT deleted");
		}

		return result;
	}
}
