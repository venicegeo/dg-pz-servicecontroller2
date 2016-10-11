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
package org.venice.piazza.servicecontroller.util;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import util.PiazzaLogger;

//@RunWith(SpringJUnit4ClassRunner.class) 

//@ComponentScan({ "org.venice.piazza.servicecontroller.util" })
@ComponentScan({ "MY_NAMESPACE, util" })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@WebAppConfiguration

public class CorePropertiesTests {
	public static String HTTP_PROTOCOL = "http";
	public static String HTTPS_PROTOCOL = "https";
	public static String DOMAIN = "TheDomain";

	
	public static String KAFKA_GROUP = "TheKafkaGroup";
	public static String KAFKA_HOST = "kafkaHost";
	public static String MONGO_COLLECTION_NAME = "TheServiceCollection";
	public static String MONGO_DB = "mongoDB";
	public static String MONGO_URI = "mongoURI";
	public static String SC_HOST = "scgeointhost";
    public static String SC_PORT="8888";   
    public static String SEARCH_DOMAIN_PREFIX = "pz-search.io";
    public static String SEARCH_DOMAIN_PORT = "8888";
    public static String SEARCH_DELETE_ENDPOINT = "/delete";
    public static String SEARCH_ENDPOINT = "/search";
    public static String SEARCH_INGEST_ENDPOINT = "/ingest";
    public static String SEARCH_INGEST_DOMAIN_PREFIX = "pz-search-metadata-ingest.io";
    public static String SEARCH_INGEST_PORT = "8080";
    public static String SEARCH_UPDATE_ENDPOINT = "/update";
    public static int SERVER_PORT=343;  
    public static String SPACE="The Space";
	
	@Autowired
	private CoreServiceProperties coreServiceProps;
	
	@Mock
	private PiazzaLogger piazzaLogger;
	
	@Before
	public void setup() {
		
	}
	@Ignore
	@Test
	/** 
	 * Test if the Service Controller Host is read correctly and populated
	 */
	public void testHost() {
		String hostName = coreServiceProps.getHost();
		assertEquals("The service controller host was autowired properly.", SC_HOST, hostName);
	}
	@Ignore
	@Test
	/** 
	 * Test if the Service Controller host is read correctly and populated
	 */
	public void testSetHost() {
		coreServiceProps.setHost(SC_HOST + "2");
		String hostName = coreServiceProps.getHost();
		assertEquals("The service controller host was been set correctly.", hostName, SC_HOST + "2");
	}
	@Ignore
	@Test
	/** 
	 * Test if the MongoDB Collection Name is read correctly and populated
	 */
	public void testMongoCollectionNameRUI() {
		String collectionName = coreServiceProps.getMongoCollectionName();
		assertEquals("The MongoDB host was autowired properly.", collectionName, MONGO_COLLECTION_NAME);
	}
	@Ignore
	@Test
	/** 
	 * Test if the MongoDB URI is read correctly and populated
	 */
	public void testSetMongoCollectionName() {
		coreServiceProps.setMongoCollectionName(MONGO_COLLECTION_NAME + "2");
		String collectionName = coreServiceProps.getMongoCollectionName();
		assertEquals("The MongoDB host was autowired properly.", collectionName, MONGO_COLLECTION_NAME + "2");
	}
	@Ignore
	@Test
	/** 
	 * Test if the MongoDB URI is read correctly and populated
	 */
	public void testMongoDBRUI() {
		String hostResult = coreServiceProps.getMongoHost();
		assertEquals("The MongoDB URI was autowired properly.", hostResult, MONGO_URI);
	}
	@Ignore
	@Test
	/** 
	 * Test if the MongoDB URI is read correctly and populated
	 */
	public void testSetMongoDBRUI() {
		coreServiceProps.setMongoHost(MONGO_URI + "2");
		String hostResult = coreServiceProps.getMongoHost();
		assertEquals("The MongoDB URI was autowired properly.", hostResult, MONGO_URI + "2");
	}
	@Ignore
	@Test
	/** 
	 * Test if the MongoDB Name is autowired properly.
	 */
	public void testMongoDBName() {
		String hostResult = coreServiceProps.getMongoDBName();
		assertEquals("The MongoDB DB Name was autowired properly.", hostResult, MONGO_DB);
	}
	@Ignore
	@Test
	/** 
	 * Test if the MongoDB Name is set properly.
	 */
	public void testSetMongoDBName() {
		coreServiceProps.setMongoDBName(MONGO_DB + "2");
		String hostResult = coreServiceProps.getMongoDBName();
		assertEquals("The MongoDB DB Name was autowired properly.", hostResult, MONGO_DB + "2");
	}

	@Ignore
	@Test
	/** 
	 * Test if the Kafka host is read correctly and populated
	 */
	public void testKafkaHost() {
		String hostResult = coreServiceProps.getKafkaHost();
		assertEquals("The Kafka host was autowired properly.", hostResult, KAFKA_HOST);
	}
	@Ignore
	@Test
	/** 
	 * Test if the Kafka host is set correctly.
	 */
	public void testSetKafkaHost() {
		coreServiceProps.setKafkaHost(KAFKA_HOST + "2");
		String theResult = coreServiceProps.getKafkaHost();
		assertEquals("The Kafka host was set properly.", KAFKA_HOST + "2", theResult);
	}
	@Ignore
	@Test
	/** 
	 * Test if the Kafka Group is read correctly and populated
	 */
	public void testKafkaGroup() {
		String group = coreServiceProps.getKafkaGroup();
		// This is how the value should be after autowiring
	    String correctValue = KAFKA_GROUP + '-' + SPACE;
		assertEquals("The Kafka group was autowired properly.", group, correctValue);
	}
	@Ignore
	@Test
	/** 
	 * Test if the Kafka Group is set correctly.
	 */
	public void testSetKafkaGroup() {
		coreServiceProps.setKafkaGroup(KAFKA_GROUP+ "2");
		String theResult = coreServiceProps.getKafkaGroup();
		assertEquals("The Kafka group was set properly.", KAFKA_GROUP + "2", theResult);
	}
	
	@Ignore
	@Test
	/** 
	 * Test if the service controller port is read correctly and populated
	 */
	public void testPort() {
		String port = coreServiceProps.getPort();
		assertEquals("The port was autowired properly.", port, SC_PORT);
	}
	@Ignore
	@Test
	/** 
	 * Test if the service controller port is set correctly.
	 */
	public void testSetPort() {
		coreServiceProps.setPort(SC_PORT);
		String port = coreServiceProps.getPort();
		assertEquals("The port was set properly.", SC_PORT, port);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz metadata delete URL is read correctly and populated
	 */
	public void testSearchMetadataDeleteUrl() {
		String deleteMetadataURL = coreServiceProps.getPzServicemetadataDeleteUrl();
		
		String actualURL = 	HTTP_PROTOCOL + "://" + SEARCH_INGEST_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_INGEST_PORT+ '/' + SEARCH_DELETE_ENDPOINT;

		assertEquals("The pz metadata delete URL was autowired properly.", deleteMetadataURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz metadata ingest URL is set correctly.
	 */
	public void testSetSearchMetadataDeleteUrl() {
		String actualURL = 	HTTP_PROTOCOL + "://" + SEARCH_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_INGEST_PORT+ '/' + SEARCH_DELETE_ENDPOINT;
		coreServiceProps.setPzServicemetadataDeleteUrl(actualURL);
		String deleteMetadataURL = coreServiceProps.getPzServicemetadataDeleteUrl();
		assertEquals("The pz metadata delete URL was set properly.", deleteMetadataURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz metadata update URL is read correctly and populated
	 */
	public void testSearchMetadataUpdateUrl() {
		String updateMetadataURL = coreServiceProps.getPzServicemetadataUpdateUrl();
		
		String actualURL = 	HTTP_PROTOCOL + "://" + SEARCH_INGEST_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_INGEST_PORT+ '/' + SEARCH_UPDATE_ENDPOINT;

		assertEquals("The pz metadata update URL was autowired properly.", updateMetadataURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz metadata ingest URL is set correctly.
	 */
	public void testSetSearchMetadataUpdateUrl() {
		String actualURL = 	HTTP_PROTOCOL + "://" + SEARCH_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_INGEST_PORT+ '/' + SEARCH_UPDATE_ENDPOINT;
		coreServiceProps.setPzServicemetadataUpdateUrl(actualURL);
		String updateMetadataURL = coreServiceProps.getPzServicemetadataUpdateUrl();
		assertEquals("The pz metadata upate URL was set properly.", updateMetadataURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz metadata ingest URL is read correctly and populated
	 */
	public void testSearchMetadataIngestUrl() {
		String ingestURL = coreServiceProps.getPzServicemetadataIngestUrl();
		
		String actualURL = 	HTTP_PROTOCOL + "://" + SEARCH_INGEST_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_INGEST_PORT+ '/' + SEARCH_INGEST_ENDPOINT;

		assertEquals("The pz metadata ingest URL was autowired properly.", ingestURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz metadata ingest URL is set correctly.
	 */
	public void testSetSearchMetadataIngestUrl() {
		String actualURL = 	HTTP_PROTOCOL + "://" + SEARCH_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_INGEST_PORT+ '/' + SEARCH_INGEST_ENDPOINT;
		coreServiceProps.setPzServicemetadataIngestUrl(actualURL);
		String ingestURL = coreServiceProps.getPzServicemetadataIngestUrl();
		assertEquals("The pz metadata ingest URL was set properly.", ingestURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz search URL is read correctly and populated
	 */
	public void testPzSearchUrl() {
		String pzsearch = coreServiceProps.getPzSearchUrl();
		
		String actualURL = 	HTTPS_PROTOCOL + "://" + SEARCH_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_DOMAIN_PORT+ '/' + SEARCH_ENDPOINT;

		assertEquals("The pz search URL was autowired properly.", pzsearch, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the pz search URL is set correctly.
	 */
	public void testSetPzSearchUrl() {
		String actualURL = 	HTTPS_PROTOCOL + "://" + SEARCH_DOMAIN_PREFIX + '.' + DOMAIN + ':' + SEARCH_DOMAIN_PORT+ '/' + SEARCH_ENDPOINT;
		coreServiceProps.setPzSearchUrl(actualURL);
		String pzsearchURL = coreServiceProps.getPzSearchUrl();
		assertEquals("The pz search URL was set properly.", pzsearchURL, actualURL);
	}
	@Ignore
	@Test
	/** 
	 * Test if the service controller port is read correctly and populated
	 */
	public void testServerPort() {
		int port = coreServiceProps.getServerPort();
		assertEquals("The server port was autowired properly.", port, SERVER_PORT);
	}
	@Ignore
	@Test
	/** 
	 * Test if the server port is set correctly.
	 */
	public void testSetServerPort() {
		coreServiceProps.setServerPort(SERVER_PORT);
		int port = coreServiceProps.getServerPort();
		assertEquals("The server port was set properly.", SERVER_PORT, port);
	}
	@Ignore
	@Test
	/** 
	 * Test if the space is read correctly and populated
	 */
	public void testSpace() {
		String space = coreServiceProps.getSpace();
		assertEquals("The space was autowired properly.", space, SPACE);
	}
	@Ignore
	@Test
	/** 
	 * Test if the space is set correctly.
	 */
	public void testSetSpace() {
		coreServiceProps.setSpace(SPACE);
		String space = coreServiceProps.getSpace();
		assertEquals("The server port was set properly.", SPACE, space);
	}

	@Configuration
	@ComponentScan({ "org.venice.piazza.servicecontroller.util" })
	
	static class CorePropertiesTestsConfig {
		

		@Bean
	    public  PropertySourcesPlaceholderConfigurer properties() throws Exception {
	        final PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
	        Properties properties = new Properties();

	        properties.setProperty("vcap.services.pz-kafka.credentials.host", KAFKA_HOST);
	        properties.setProperty("vcap.services.pz-mongodb.credentials.uri", MONGO_URI);
	        properties.setProperty("vcap.services.pz-mongodb.credentials.database", MONGO_DB);

            properties.setProperty("logger.protocol", HTTPS_PROTOCOL);
            properties.setProperty("logger.prefix", "pz-logger");
            properties.setProperty("DOMAIN", DOMAIN);
            properties.setProperty("logger.port", "333");
            properties.setProperty("logger.endpoint", "endpoint");
            properties.setProperty("uuid.url", "HTTPS_PROTOCOL");
            properties.setProperty("uuid.endpoint", "endpoint");
            properties.setProperty("SPACE", SPACE);
            properties.setProperty("kafka.group",  KAFKA_GROUP);
            properties.setProperty("server.port",  new Integer(SERVER_PORT).toString());
            properties.setProperty("mongo.db.collection.name", MONGO_COLLECTION_NAME);
            properties.setProperty("servicecontroller.host", SC_HOST);
            properties.setProperty("servicecontroller.port", new Integer(SC_PORT).toString());
//            properties.setProperty("search.protocol", HTTPS_PROTOCOL);
//            properties.setProperty("search.prefix", SEARCH_DOMAIN_PREFIX);
//            properties.setProperty("search.port", SEARCH_DOMAIN_PORT);
//            properties.setProperty("search.endpoint", SEARCH_ENDPOINT);
//            properties.setProperty("metadata.ingest.protocol", HTTP_PROTOCOL);
//            properties.setProperty("metadata.ingest.prefix", SEARCH_INGEST_DOMAIN_PREFIX);
//            properties.setProperty("metadata.ingest.port", SEARCH_INGEST_PORT);
//            properties.setProperty("metadata.ingest.endpoint", SEARCH_INGEST_ENDPOINT);
//            properties.setProperty("metadata.update.endpoint", SEARCH_UPDATE_ENDPOINT);
//            properties.setProperty("metadata.delete.endpoint", SEARCH_DELETE_ENDPOINT);
            


	        pspc.setProperties(properties);
	        return pspc;
	    }


	}

}
