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
package org.venice.piazza.servicecontroller.elasticsearch.accessors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.type.ServiceMetadataIngestJob;
import model.logger.Severity;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.service.metadata.Service;
import util.PiazzaLogger;

/**
 * 
 * @author mlynum & Sonny.Saniev
 *
 */
@Component
public class ElasticSearchAccessor {
	private String SERVICEMETADATA_INGEST_URL;
	private String SERVICEMETADATA_UPDATE_URL;
	private String SERVICEMETADATA_DELETE_URL;
	@Autowired
	private RestTemplate restTemplate;
	@Autowired
	private PiazzaLogger logger;
	
	@Autowired
	private CoreServiceProperties coreServiceProperties;

	private final static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchAccessor.class);

	public ElasticSearchAccessor() {
	}

	@PostConstruct
	private void initialize() {
		SERVICEMETADATA_INGEST_URL = coreServiceProperties.getPzServicemetadataIngestUrl();
		SERVICEMETADATA_UPDATE_URL = coreServiceProperties.getPzServicemetadataUpdateUrl();
		SERVICEMETADATA_DELETE_URL = coreServiceProperties.getPzServicemetadataDeleteUrl();
		logger.log("Search endpoint is " + SERVICEMETADATA_INGEST_URL, Severity.DEBUG);

	}

	/**
	 * Dispatches request to elastic search for service updates
	 * 
	 * @param service
	 *            Service object
	 * @return PiazzaResponse
	 */
	public PiazzaResponse save(Service service) {
		logger.log("Saving service " + service.getServiceId() + " " + SERVICEMETADATA_INGEST_URL, Severity.DEBUG);
		return dispatchElasticSearch(service, SERVICEMETADATA_INGEST_URL);
	}

	/**
	 * Dispatches request to elastic search for service updates
	 * 
	 * @param service
	 *            Service object
	 * @return PiazzaResponse
	 */
	public PiazzaResponse update(Service service) {
		logger.log("Saving service " + service.getServiceId() + " " + SERVICEMETADATA_UPDATE_URL, Severity.DEBUG);

		return dispatchElasticSearch(service, SERVICEMETADATA_UPDATE_URL);
	}
	
	/**
	 * Dispatches request to elastic search for service updates
	 * 
	 * @param service
	 *            Service object
	 * @return PiazzaResponse
	 */
	public PiazzaResponse delete(Service service) {
		logger.log("Saving service " + service.getServiceId() + " " + SERVICEMETADATA_DELETE_URL, Severity.DEBUG);
		return dispatchElasticSearch(service, SERVICEMETADATA_DELETE_URL);
	}
	
	/**
	 * Private method to post requests to elastic search for
	 * registering / updating / deleting the service metadata.
	 * 
	 * @param Service object
	 * @param url
	 *            elastic search endpoints to post to
	 * @return PiazzaResponse response
	 */
	private PiazzaResponse dispatchElasticSearch(Service service, String url) {
		try {
			ServiceMetadataIngestJob job = new ServiceMetadataIngestJob();
			
			job.setData(service);
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			
			//CSS 10/24 TBD, this obsolete artifact of "job" parameter caused Elasticsearch trouble bug #9336
			//HttpEntity<ServiceMetadataIngestJob> entity = new HttpEntity<ServiceMetadataIngestJob>(job, headers);
			HttpEntity<Service> entity = new HttpEntity<Service>(service, headers);

			return restTemplate.postForObject(url, entity, PiazzaResponse.class);
		} catch (Exception exception) {
			String error = String.format("Could not Index ServiceMetaData to Service: %s", exception.getMessage());
			LOGGER.error(error, exception);
			logger.log(error, Severity.ERROR);
			return new ErrorResponse("Error connecting to ServiceMetadata Service: " + exception.getMessage(), "ServiceController");
		}
	}
}
