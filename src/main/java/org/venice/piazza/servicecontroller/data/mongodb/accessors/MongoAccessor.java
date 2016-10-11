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
package org.venice.piazza.servicecontroller.data.mongodb.accessors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.joda.time.DateTime;
import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.DBQuery.Query;
import org.mongojack.DBSort;
import org.mongojack.DBUpdate;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.async.AsyncServiceInstance;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;

import model.job.metadata.ResourceMetadata;
import model.response.Pagination;
import model.response.PiazzaResponse;
import model.response.ServiceListResponse;
import model.service.SearchCriteria;
import model.service.metadata.Service;
import util.PiazzaLogger;

/**
 * Class to store service information in MongoDB.
 * 
 * @author mlynum
 * 
 */
// TODO FUTURE See a better way to abstract out MongoDB
// TODO FUTURE See a way to store service controller internals
@Component
public class MongoAccessor {
	@Value("${mongo.thread.multiplier}")
	private int mongoThreadMultiplier;
	private String DATABASE_HOST;
	private String DATABASE_NAME;
	private String SERVICE_COLLECTION_NAME;
	private static final String ASYNC_INSTANCE_COLLECTION_NAME = "AsyncServiceInstances";
	private MongoClient mongoClient;

	@Value("${async.stale.instance.threshold.seconds}")
	private int STALE_INSTANCE_THRESHOLD_SECONDS;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private CoreServiceProperties coreServiceProperties;

	private final static Logger LOGGER = LoggerFactory.getLogger(MongoAccessor.class);

	public MongoAccessor() {
	}

	@PostConstruct
	private void initialize() {
		// Initialize the MongoDB
		DATABASE_HOST = coreServiceProperties.getMongoHost();
		DATABASE_NAME = coreServiceProperties.getMongoDBName();
		SERVICE_COLLECTION_NAME = coreServiceProperties.getMongoCollectionName();
		LOGGER.debug("====================================================");
		LOGGER.debug("DATABASE_HOST=" + DATABASE_HOST);
		LOGGER.debug("DATABASE_NAME=" + DATABASE_NAME);
		LOGGER.debug("SERVICE_COLLECTION_NAME=" + SERVICE_COLLECTION_NAME);
		LOGGER.debug("====================================================");

		try {
			mongoClient = new MongoClient(new MongoClientURI(DATABASE_HOST + "?waitQueueMultiple=" + mongoThreadMultiplier));
		} catch (Exception ex) {
			LOGGER.error(ex.getMessage());
			String message = String.format("Error Contacting Mongo Host %s: %s", DATABASE_HOST, ex.getMessage());
			logger.log(message, PiazzaLogger.ERROR);
			LOGGER.debug(ex.toString());

		}
	}

	@PreDestroy
	private void close() {
		mongoClient.close();
	}

	/**
	 * Gets a reference to the MongoDB Client Object.
	 * 
	 * @return
	 */
	public MongoClient getClient() {
		return mongoClient;
	}

	/**
	 * updateservice information
	 */
	public String update(Service sMetadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);

			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class, String.class);

			Query query = DBQuery.is("serviceId", sMetadata.getServiceId());

			WriteResult<Service, String> writeResult = coll.update(query, sMetadata);
			logger.log("The result is", writeResult.toString());
			// Return the id that was used
			return sMetadata.getServiceId().toString();

		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			String message = String.format("Error Updating Mongo Service entry : %s", ex.getMessage());
			logger.log(message, PiazzaLogger.ERROR);
			LOGGER.error(Arrays.toString(ex.getStackTrace()));
		}

		return result;
	}

	/**
	 * Deletes existing registered service from mongoDB
	 * 
	 * @param serviceId
	 *            Service id to be deleted
	 * @param softDelete
	 *            If softDelete is true, updates status instead of deleting.
	 *
	 * @return message string containing result
	 */
	public String delete(String serviceId, boolean softDelete) {
		String result = "service " + serviceId + " NOT deleted ";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);

			if (softDelete) {
				JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class, String.class);
				Query query = DBQuery.is("serviceId", serviceId);
				WriteResult<Service, String> writeResult = coll.update(query,
						DBUpdate.set("resourceMetadata.availability", ResourceMetadata.STATUS_TYPE.OFFLINE.toString()));
				int recordsChanged = writeResult.getN();

				// Return the id that was used
				if (recordsChanged == 1) {
					result = " service " + serviceId + " deleted ";
				}
			} else {
				// Delete the existing entry for the Job
				BasicDBObject deleteQuery = new BasicDBObject();
				deleteQuery.append("serviceId", serviceId);
				collection.remove(deleteQuery);
				result = " service " + serviceId + " deleted ";
			}

			return result;
		} catch (MongoException ex) {

			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			String message = String.format("Error Deleting Mongo Service entry : %s", ex.getMessage());
			logger.log(message, PiazzaLogger.ERROR);
		}

		return result;
	}

	/**
	 * Store the new service information
	 */
	public String save(Service sMetadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);

			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class, String.class);

			WriteResult<Service, String> writeResult = coll.insert(sMetadata);
			// Return the id that was used
			return sMetadata.getServiceId();

		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			String message = String.format("Error Saving Mongo Service entry : %s", ex.getMessage());
			logger.log(message, PiazzaLogger.ERROR);

		}

		return result;
	}

	/**
	 * List services
	 */
	public List<Service> list() {
		ArrayList<Service> result = new ArrayList<Service>();
		try {

			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);

			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class, String.class);

			DBCursor<Service> metadataCursor = coll
					.find(DBQuery.notEquals("resourceMetadata.availability", ResourceMetadata.STATUS_TYPE.OFFLINE.toString()));
			while (metadataCursor.hasNext()) {
				result.add(metadataCursor.next());
			}
			// Return the id that was used
			return result;

		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			String message = String.format("Error Listing Mongo Service entries : %s", ex.getMessage());
			logger.log(message, PiazzaLogger.ERROR);

		}

		return result;
	}

	/**
	 * Gets the Resource collection that contains the Services.
	 * 
	 * @return Service Resource Collection
	 */
	public JacksonDBCollection<Service, String> getServicesCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Service.class, String.class);
	}

	/**
	 * Gets a reference to the MongoDB's Job Collection.
	 * 
	 * @return
	 */
	public JacksonDBCollection<Service, String> getServiceCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Service.class, String.class);
	}

	/**
	 * Get a list of services with pagination
	 */

	public PiazzaResponse getServices(Integer page, Integer perPage, String order, String sortBy, String keyword, String userName) {
		// Create the Query
		Query query = DBQuery.empty();

		// Keyword clause, if provided
		if ((keyword != null) && (keyword.isEmpty() == false)) {
			Pattern regex = Pattern.compile(String.format("(?i)%s", keyword));
			// Querying specific fields for the keyword
			query.or(DBQuery.regex("resourceMetadata.name", regex), DBQuery.regex("resourceMetadata.description", regex),
					DBQuery.regex("url", regex), DBQuery.regex("serviceId", regex));
		}

		// Username clause, if provided
		if ((userName != null) && (userName.isEmpty() == false)) {
			query.and(DBQuery.is("resourceMetadata.createdBy", userName));
		}

		// Execute the Query
		DBCursor<Service> cursor = getServiceCollection().find(query);

		// Sort and order the Results
		if (order.equalsIgnoreCase("asc")) {
			cursor = cursor.sort(DBSort.asc(sortBy));
		} else if (order.equalsIgnoreCase("desc")) {
			cursor = cursor.sort(DBSort.desc(sortBy));
		}

		// Get the total count
		Integer size = new Integer(cursor.size());

		// Paginate the results
		List<Service> data = cursor.skip(page * perPage).limit(perPage).toArray();

		// Attach pagination information
		Pagination pagination = new Pagination(size, page, perPage, sortBy, order);

		// Create the Response and send back
		return new ServiceListResponse(data, pagination);
	}

	/**
	 * Returns a ResourceMetadata object that matches the specified Id.
	 * 
	 * @param jobId
	 *            Job Id
	 * @return The Job with the specified Id
	 */
	public Service getServiceById(String serviceId) throws ResourceAccessException {
		BasicDBObject query = new BasicDBObject("serviceId", serviceId);
		Service service;

		try {
			if ((service = getServiceCollection().findOne(query)) == null) {
				throw new ResourceAccessException("Service not found.");
			}
		} catch (MongoTimeoutException mte) {
			throw new ResourceAccessException("MongoDB instance not available.");
		}

		return service;
	}

	/**
	 * Returns a list of ResourceMetadata based on the criteria provided
	 * 
	 * @return List of matching services that match the search criteria
	 */
	public List<Service> search(SearchCriteria criteria) {
		List<Service> results = new ArrayList<Service>();
		if (criteria != null) {

			LOGGER.debug("Criteria field=" + criteria.getField());
			LOGGER.debug("Criteria field=" + criteria.getPattern());

			Pattern pattern = Pattern.compile(criteria.pattern);
			BasicDBObject query = new BasicDBObject(criteria.field, pattern);

			try {

				DBCursor<Service> cursor = getServiceCollection().find(query);
				while (cursor.hasNext()) {
					results.add(cursor.next());
				}

				// Now try to look for the field in the resourceMetadata just to make sure

				query = new BasicDBObject("resourceMetadata." + criteria.field, pattern);
				cursor = getServiceCollection().find(query);
				while (cursor.hasNext()) {
					Service serviceItem = cursor.next();
					if (!exists(results, serviceItem.getServiceId()))
						results.add(serviceItem);
				}

			} catch (MongoTimeoutException mte) {
				throw new ResourceAccessException("MongoDB instance not available.");
			}
		}

		return results;
	}

	/**
	 * Checks to see if the result was already found
	 * 
	 * @return true - result is already there false - result has not been found
	 */
	private boolean exists(List<Service> serviceResults, String id) {
		boolean doesExist = false;

		for (int i = 0; i < serviceResults.size(); i++) {
			String serviceItemId = serviceResults.get(i).getServiceId();
			if (serviceItemId.equals(id))
				doesExist = true;
		}
		return doesExist;

	}

	/**
	 * Gets the Async Service Instance collection that contains the current instances of running services..
	 * 
	 * @return Service Instance Collection
	 */
	private JacksonDBCollection<AsyncServiceInstance, String> getAsyncServiceInstancesCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(ASYNC_INSTANCE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, AsyncServiceInstance.class, String.class);
	}

	/**
	 * Adds an Asynchronous Service Instance to the Database.
	 * 
	 * @param instance
	 *            The instance
	 */
	public void addAsyncServiceInstance(AsyncServiceInstance instance) {
		getAsyncServiceInstancesCollection().insert(instance);
	}

	/**
	 * @return Gets all Instances that require a status check.
	 */
	public List<AsyncServiceInstance> getStaleServiceInstances() {
		// Get the time to query. Threshold seconds ago, in epoch.
		long thresholdEpoch = new DateTime().minusSeconds(STALE_INSTANCE_THRESHOLD_SECONDS).getMillis();
		// Query for all results that are older than the threshold time
		DBCursor<AsyncServiceInstance> cursor = getAsyncServiceInstancesCollection()
				.find(DBQuery.lessThan("lastCheckedOn", thresholdEpoch));
		return cursor.toArray();
	}

	/**
	 * Gets the Async Service Instance for the Piazza Job ID
	 * 
	 * @param jobId
	 *            The piazza Job ID
	 * @return The async service instance
	 */
	public AsyncServiceInstance getInstanceByJobId(String jobId) {
		BasicDBObject query = new BasicDBObject("jobId", jobId);
		AsyncServiceInstance instance = getAsyncServiceInstancesCollection().findOne(query);
		return instance;
	}

	/**
	 * Updates an Async Service instance.
	 */
	public void updateAsyncServiceInstance(AsyncServiceInstance instance) {
		getAsyncServiceInstancesCollection().update(DBQuery.is("jobId", instance.getJobId()), instance);
	}

	/**
	 * Deletese an Async Service Instance by Job ID. This is done when the Service has been processed to completion and
	 * the instance is no longer needed.
	 * 
	 * @param id
	 *            The Job ID of the Async Service Instance.
	 */
	public void deleteAsyncServiceInstance(String jobId) {
		getAsyncServiceInstancesCollection().remove(DBQuery.is("jobId", jobId));
	}
}