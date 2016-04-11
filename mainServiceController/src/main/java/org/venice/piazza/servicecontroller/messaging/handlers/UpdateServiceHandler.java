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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.PiazzaJobType;
import model.job.type.UpdateServiceJob;
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

public class UpdateServiceHandler implements PiazzaJobHandler {
	private MongoAccessor accessor;
	private PiazzaLogger coreLogger;
	private UUIDFactory uuidFactory;
	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateServiceHandler.class);


	public UpdateServiceHandler(MongoAccessor accessor, CoreServiceProperties coreServiceProp, PiazzaLogger coreLogger, UUIDFactory uuidFactory){ 
		this.accessor = accessor;
		this.coreLogger = coreLogger;
		this.uuidFactory = uuidFactory;
	
	}

    /*
     * Handler for the RegisterServiceJob  that was submitted.  Stores the metadata in
     * MongoDB
     * (non-Javadoc)
     * @see org.venice.piazza.servicecontroller.messaging.handlers.Handler#handle(model.job.PiazzaJobType)
     */
	public ResponseEntity<List<String>> handle (PiazzaJobType jobRequest ) {
		
		LOGGER.debug("Updating a service");
		UpdateServiceJob job = (UpdateServiceJob)jobRequest;
		if (job != null)  {
			// Get the ResourceMetadata
			Service sMetadata = job.data;
			LOGGER.info("serviceMetadata received is " + sMetadata);
			coreLogger.log("serviceMetadata received is " + sMetadata, coreLogger.INFO);

			String result = handle(sMetadata);
			if (result.length() > 0) {
				String jobId = job.getJobId();
				// TODO Use the result, send a message with the resource ID
				// and jobId
				ArrayList<String> resultList = new ArrayList<String>();
				resultList.add(jobId);
				resultList.add(sMetadata.getId());
				ResponseEntity<List<String>> handleResult = new ResponseEntity<List<String>>(resultList,HttpStatus.OK);
				return handleResult;
				
			}
			else {
				LOGGER.error("No result response from the handler, something went wrong");
				coreLogger.log("No result response from the handler, something went wrong", coreLogger.ERROR);
				ArrayList<String> errorList = new ArrayList<String>();
				errorList.add("UpdateServiceHandler handle didn't work");
				ResponseEntity<List<String>> errorResult = new ResponseEntity<List<String>>(errorList,HttpStatus.METHOD_FAILURE);
				
				return errorResult;
			}
		}
		else {
			return null;
		}
	}//handle
	
	/**
	 * 
	 * @param rMetadata
	 * @return resourceID of the registered service
	 */
	public String handle (Service sMetadata) {

        coreLogger.log("about to update a registered service.", PiazzaLogger.INFO);
        LOGGER.info("about to update a registered service.");

		
		String result = accessor.update(sMetadata);
		LOGGER.debug("The result of the update is " + result);
		if (result.length() > 0) {
		   coreLogger.log("The service " + sMetadata.getName() + " was updated with id " + result, PiazzaLogger.INFO);
		   LOGGER.info("The service " + sMetadata.getName() + " was updated with id " + result);
		} else {
			   coreLogger.log("The service " + sMetadata.getName() + " was NOT updated", PiazzaLogger.INFO);
			   LOGGER.info("The service " + sMetadata.getName() + " was NOT updated");
		}
		// If an ID was returned then send a kafka message back updating the job iD 
		// with the resourceID
		return result;
				
	}
	


}
