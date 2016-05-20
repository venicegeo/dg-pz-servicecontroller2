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
import org.springframework.beans.factory.annotation.Value;
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

@Component
@DependsOn("coreInitDestroy")
public class ElasticSearchAccessor {
	private String SEARCH_URL;
	private String SERVICEMETADATA_INGEST_URL;
	private String SERVICEMETADATA_UPDATE_URL;
	private static final String DEFAULT_PAGE_SIZE = "10";
	private static final String DEFAULT_PAGE = "0";
	private RestTemplate restTemplate = new RestTemplate();
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private CoreServiceProperties coreServiceProperties;

	/**
	 * Store the new service information
	 */

	public ElasticSearchAccessor() {
	}

	@PostConstruct
	private void initialize() {
		SEARCH_URL = coreServiceProperties.getPzSearchUrl();
		SERVICEMETADATA_INGEST_URL = coreServiceProperties.getPzServicemetadataIngestUrl();
		SERVICEMETADATA_UPDATE_URL = coreServiceProperties.getPzServicemetadataUpdateUrl();

	}

	public PiazzaResponse save(Service sMetadata) {

		ServiceMetadataIngestJob job = new ServiceMetadataIngestJob();
		job.setData(sMetadata);

		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<ServiceMetadataIngestJob> entity = new HttpEntity<ServiceMetadataIngestJob>(job, headers);
			PiazzaResponse servicemetadataIngestResponse = restTemplate.postForObject(
					String.format("%s", SERVICEMETADATA_INGEST_URL), entity, PiazzaResponse.class);
			logger.log(String.format("Indexed ServiceMetadata from Gateway."), PiazzaLogger.INFO);
			return servicemetadataIngestResponse;
		} catch (Exception exception) {
			logger.log(String.format("Could not Index ServiceMetaData to Service: %s", exception.getMessage()),
					PiazzaLogger.ERROR);
			return new ErrorResponse(null, "Error connecting toServiceMetadata Ingest Service: "
					+ exception.getMessage(), "ServiceController");
		}

	}

	public PiazzaResponse update(Service sMetadata) {
		ServiceMetadataIngestJob job = new ServiceMetadataIngestJob();
		job.setData(sMetadata);

		try {
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<ServiceMetadataIngestJob> entity = new HttpEntity<ServiceMetadataIngestJob>(job, headers);
			PiazzaResponse servicemetadataIngestResponse = restTemplate.postForObject(
					String.format("%s", SERVICEMETADATA_UPDATE_URL), entity, PiazzaResponse.class);
			logger.log(String.format("Indexed ServiceMetadata from Gateway."), PiazzaLogger.INFO);
			return servicemetadataIngestResponse;
		} catch (Exception exception) {
			logger.log(String.format("Could not Index ServiceMetaData to Service: %s", exception.getMessage()),
					PiazzaLogger.ERROR);
			return new ErrorResponse(null, "Error connecting toServiceMetadata Update Service: "
					+ exception.getMessage(), "ServiceController");
		}

	}

	// TODO - Need to create a delete service Job and new elasticsearch endpoint
	// for it
	public void delete(String serviceId) {

	}

}