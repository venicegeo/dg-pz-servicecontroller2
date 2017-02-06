/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *l
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller.messaging.handlers;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import model.job.metadata.ResourceMetadata;
import model.job.type.SearchServiceJob;
import model.logger.Severity;
import model.service.SearchCriteria;
import model.service.metadata.Service;
import util.PiazzaLogger;
/**
 * @author mlynum
 * @version 1.0
 */

public class SearchServiceHandlerTest {
	List <Service> services = null;
	
	@Mock 
	private PiazzaLogger loggerMock;
	@Mock
	private MongoAccessor accessorMock;

	@Mock
	private ObjectMapper omMock;

	@InjectMocks
	private SearchServiceHandler ssHandler;
	
	@Before
    public void setup() {
		
		// Create some services
		Service service;
		ResourceMetadata rm;
		services = new ArrayList<>();
        // Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toUpper Params";
		rm.description = "Service to convert string to uppercase";

		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("a842aae2-bd74-4c4b-9a65-c45e8cd9060");
		service.setUrl("http://localhost:8082/jumpstart/string/toUpper");
		MockitoAnnotations.initMocks(this);	
		
		services.add(service);
		
		// Setup a Service with some Resource Metadata
		rm = new ResourceMetadata();
		rm.name = "toLower Params";
		rm.description = "Service to convert string to lower";

		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("a842bte2-bd74-4c4b-9a65-c45e8cdu91");
		service.setUrl("http://localhost:8082/jumpstart/string/toLower");
		services.add(service);
		
		rm = new ResourceMetadata();
		rm.name = "convert";
		rm.description = "Converts a string to UPPER or LOWER case. POST method";
		
		service = new Service();
		service.setMethod ("POST");
		service.setResourceMetadata(rm);
		service.setServiceId("9842bze2-bd74-4c4b-0a65-c45e8cdu91");
		service.setUrl("http://localhost:8082/umpstart/string/convert");
		
		services.add(service);
		MockitoAnnotations.initMocks(this);	
		
    }
	
	/**
	 * Test that the search retrieves results
	 */
	@Test
	public void testSearchSuccess() {
		SearchServiceJob job = new SearchServiceJob();
		
		SearchCriteria criteria = new SearchCriteria();
		criteria.field="animalType";
		criteria.pattern="A*r";
	
		job.data =criteria;
		try {
			ObjectMapper mapper = new ObjectMapper();
			String responseServiceString = mapper.writeValueAsString(services);
			

			ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);

			final SearchServiceHandler ssMock = Mockito.spy (ssHandler);

			Mockito.doReturn(responseEntity).when(ssMock).handle(criteria);				
			ResponseEntity<String> result = ssMock.handle(job);
		
			assertEquals ("The response entity is correct", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct", responseEntity.getBody(), responseServiceString);


		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		}

	}
	
	/**
	 * Test that there is a failure when trying to send in a null job
	 */
	@Test
	public void testSearchNullValue() {
		SearchServiceJob job = null;
	
		ResponseEntity<String> result = ssHandler.handle(job);
		assertEquals ("The response code is 404", result.getStatusCode(), HttpStatus.BAD_REQUEST);

	}
	
	/**
	 * Test that services were returned
	 */
	@Test
	public void testSuccessSearchCriteria() {	
		try {
			ObjectMapper mapper = new ObjectMapper();
			String responseServiceString = mapper.writeValueAsString(services);
			SearchCriteria criteria = new SearchCriteria();
			criteria.field="animalType";
			criteria.pattern="A*r";

			ResponseEntity<String> responseEntity = new  ResponseEntity<String>(responseServiceString, HttpStatus.OK);
			Mockito.doReturn(services).when(accessorMock).search(criteria);		
	        //Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Severity.INFORMATIONAL);

			ResponseEntity<String> result = ssHandler.handle(criteria);
		
			assertEquals ("The response entity result is correct", responseEntity, result);
			assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.OK);
			assertEquals ("The body of the response is correct", responseEntity.getBody(), responseServiceString);


		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		}

	}
	
	/**
	 * Test Null Criteria
	 */
	@Test
	public void testNullCriteria() {	
		SearchCriteria criteria = null;

		ResponseEntity<String> responseEntity = new  ResponseEntity<String>("No criteria was specified", HttpStatus.NO_CONTENT);
		Mockito.doReturn(services).when(accessorMock).search(criteria);		

		ResponseEntity<String> result = ssHandler.handle(criteria);
	
		assertEquals ("The response code is correct", responseEntity.getStatusCode(), HttpStatus.NO_CONTENT);
		assertEquals ("The body of the response is correct", responseEntity.getBody(), result.getBody());



	}
	
	/**
	 * Test that services were returned
	 */
	@Test
	public void testSearchNoResults() {	

		SearchCriteria criteria = new SearchCriteria();
		criteria.field="animalType";
		criteria.pattern="A*r";
		List <String>results = new ArrayList<>();
		String actualResponse = "No results were returned searching for field";
		ResponseEntity<String> responseEntity = new  ResponseEntity<String>(actualResponse, HttpStatus.NO_CONTENT);
		Mockito.doReturn(results).when(accessorMock).search(criteria);		
        //Mockito.doNothing().when(loggerMock).log(Mockito.anyString(), Severity.INFORMATIONAL);

		ResponseEntity<String> result = ssHandler.handle(criteria);
	
		assertEquals ("The response entity result is correct", responseEntity, result);
		assertEquals ("The response code is 200", responseEntity.getStatusCode(), HttpStatus.NO_CONTENT);
		assertEquals ("The body of the response is correct", responseEntity.getBody(), actualResponse);



	}
	
	/**
	 * Test that the results throws a JSON exception
	 * due to a marshalling error
	 */
	@Test
	public void testThrowException() {
		ResponseEntity<String> responseEntity = new ResponseEntity<String>("Could not search for services" , HttpStatus.NOT_FOUND);

		SearchCriteria criteria = new SearchCriteria();
		criteria.field="animalType";
		criteria.pattern="A*r";
		try {
			final SearchServiceHandler ssMock = Mockito.spy (ssHandler);

			Mockito.doReturn(omMock).when(ssMock).makeObjectMapper();
			Mockito.doReturn(services).when(accessorMock).search(criteria);	
			
			Mockito.when(omMock.writeValueAsString(services)).thenThrow( new JsonMappingException("Test Exception") );
			ResponseEntity<String> result = ssMock.handle(criteria);

			
			assertEquals ("The response entity was correct for the search request", responseEntity, result);
			assertEquals ("The response code is 404", responseEntity.getStatusCode(), HttpStatus.NOT_FOUND);
			assertEquals ("The body of the response is correct", responseEntity.getBody(),  "Could not search for services");
		} catch (JsonProcessingException jpe) {
			jpe.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	

}
