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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import model.job.type.ServiceMetadataIngestJob;
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
@DependsOn("coreInitDestroy")
public class ElasticSearchAccessor {
	private String SERVICEMETADATA_INGEST_URL;
	private String SERVICEMETADATA_UPDATE_URL;
	private String SERVICEMETADATA_DELETE_URL;
	
	private RestTemplate restTemplate = new RestTemplate();
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private CoreServiceProperties coreServiceProperties;

	public ElasticSearchAccessor() {
	}

	@PostConstruct
	private void initialize() {
		SERVICEMETADATA_INGEST_URL = coreServiceProperties.getPzServicemetadataIngestUrl();
		SERVICEMETADATA_UPDATE_URL = coreServiceProperties.getPzServicemetadataUpdateUrl();
		SERVICEMETADATA_DELETE_URL = coreServiceProperties.getPzServicemetadataDeleteUrl();
	}

	/**
	 * Dispatches request to elastic search for service updates
	 * 
	 * @param service
	 *            Service object
	 * @return PiazzaResponse
	 */
	public PiazzaResponse save(Service service) {
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
			HttpEntity<ServiceMetadataIngestJob> entity = new HttpEntity<ServiceMetadataIngestJob>(job, headers);

			return restTemplate.postForObject(url, entity, PiazzaResponse.class);
		} catch (Exception exception) {
			logger.log(String.format("Could not Index ServiceMetaData to Service: %s", exception.getMessage()), PiazzaLogger.ERROR);
			return new ErrorResponse(null, "Error connecting to ServiceMetadata Service: " + exception.getMessage(), "ServiceController");
		}
	}
}
