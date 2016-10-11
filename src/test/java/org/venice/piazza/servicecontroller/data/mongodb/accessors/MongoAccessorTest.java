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

package org.venice.piazza.servicecontroller.data.mongodb.accessors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import model.job.metadata.ResourceMetadata;
import model.security.SecurityClassification;
import model.service.metadata.Service;

public class MongoAccessorTest {
	
	@InjectMocks
	private MongoAccessor mongoAccessor;

	/**
	 * Setup
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * Testing save, fail normal due to lack of mongo connection.
	 */
	@Test(expected = Exception.class)
	public void testClientSave() {
		SecurityClassification sc = new SecurityClassification();
		sc.setClassification("UNCLASSIFIED");
		ResourceMetadata rm = new ResourceMetadata();
		rm.setName("testname");
		rm.setDescription("test description");
		rm.setClassType(sc);
		
		Service service = new Service();
		service.setContractUrl("www.google.com");
		service.setServiceId(String.valueOf((int)(Math.random() * 1000000)));
		service.setUrl("www.google.com");
		service.setResourceMetadata(rm);
		service.setMethod("GET");
		
		mongoAccessor.save(service);
	}
	
	/**
	 * Testing client
	 */
	@Test
	public void testClient() {
		mongoAccessor.getClient();
	}
}
