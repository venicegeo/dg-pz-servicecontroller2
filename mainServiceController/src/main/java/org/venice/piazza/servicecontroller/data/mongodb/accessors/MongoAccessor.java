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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;

import exception.InvalidInputException;
import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.Pagination;
import model.response.PiazzaResponse;
import model.response.ServiceListResponse;
import model.service.SearchCriteria;
import model.service.metadata.Service;
import model.service.taskmanaged.ServiceJob;
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
	@Value("${vcap.services.pz-mongodb.credentials.database}")
	private String DATABASE_NAME;
	@Value("${vcap.services.pz-mongodb.credentials.host}")
	private String DATABASE_HOST;
	@Value("${vcap.services.pz-mongodb.credentials.port}")
	private int DATABASE_PORT;
	@Value("${vcap.services.pz-mongodb.credentials.username}")
	private String DATABASE_USERNAME;
	@Value("${vcap.services.pz-mongodb.credentials.password}")
	private String DATABASE_CREDENTIAL;

	private String SERVICE_COLLECTION_NAME;
	private static final String ASYNC_INSTANCE_COLLECTION_NAME = "AsyncServiceInstances";
	private MongoClient mongoClient;
	@Value("${mongo.db.servicequeue.collection.prefix}")
	private String SERVICE_QUEUE_COLLECTION_PREFIX;
	@Value("${mongo.db.job.collection.name}")
	private String JOB_COLLECTION_NAME;

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
		SERVICE_COLLECTION_NAME = coreServiceProperties.getMongoCollectionName();
		LOGGER.debug("====================================================");
		LOGGER.debug("DATABASE_HOST=" + DATABASE_HOST);
		LOGGER.debug("DATABASE_NAME=" + DATABASE_NAME);
		LOGGER.debug("SERVICE_COLLECTION_NAME=" + SERVICE_COLLECTION_NAME);
		LOGGER.debug("====================================================");

		try {
			MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
			builder.sslEnabled(true);
			builder.sslInvalidHostNameAllowed(true);
			mongoClient = new MongoClient(
					new ServerAddress(DATABASE_HOST, DATABASE_PORT),
					Arrays.asList(MongoCredential.createCredential(DATABASE_USERNAME, DATABASE_NAME, DATABASE_CREDENTIAL.toCharArray())),
					builder.threadsAllowedToBlockForConnectionMultiplier(mongoThreadMultiplier).build());
		} catch (Exception ex) {
			String message = String.format("Error Contacting Mongo Host %s: %s", DATABASE_HOST, ex.getMessage());
			logger.log(message, Severity.ERROR);
			LOGGER.error(message, ex);
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
			logger.log(String.format("%s %s", "The result is", writeResult.toString()), Severity.INFORMATIONAL);

			logger.log(String.format("Updating resource in MongoDB %s", sMetadata.getServiceId()), Severity.INFORMATIONAL,
					new AuditElement("serviceController", "Updated Service", sMetadata.getServiceId()));

			// Return the id that was used
			return sMetadata.getServiceId().toString();
		} catch (MongoException ex) {
			String message = String.format("Error Updating Mongo Service entry : %s", ex.getMessage());
			logger.log(message, Severity.ERROR);
			LOGGER.error(message, ex);
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
				// If any Service Queue exists, also delete that here.
				deleteServiceQueue(serviceId);
			}

			logger.log(String.format("Deleting resource from MongoDB %s", serviceId), Severity.INFORMATIONAL,
					new AuditElement("serviceController", "Deleted Service", serviceId));

			return result;
		} catch (MongoException ex) {
			String message = String.format("Error Deleting Mongo Service entry : %s", ex.getMessage());
			logger.log(message, Severity.ERROR);
			LOGGER.error(message, ex);
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

			logger.log(String.format("Saving resource in MongoDB %s", sMetadata.getServiceId()), Severity.INFORMATIONAL,
					new AuditElement("serviceController", "Created Service ", sMetadata.getServiceId()));

			// Return the id that was used
			return sMetadata.getServiceId();
		} catch (MongoException ex) {
			String message = String.format("Error Saving Mongo Service entry : %s", ex.getMessage());
			logger.log(message, Severity.ERROR);
			LOGGER.error(message, ex);
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
			String message = String.format("Error Listing Mongo Service entries : %s", ex.getMessage());
			logger.log(message, Severity.ERROR);
			LOGGER.error(message, ex);
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
			LOGGER.error("MongoDB instance not available", mte);
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
				LOGGER.error("MongoDB instance not available", mte);
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
	 * Deletes an Async Service Instance by Job ID. This is done when the Service has been processed to completion and
	 * the instance is no longer needed.
	 * 
	 * @param id
	 *            The Job ID of the Async Service Instance.
	 */
	public void deleteAsyncServiceInstance(String jobId) {
		getAsyncServiceInstancesCollection().remove(DBQuery.is("jobId", jobId));
	}

	/**
	 * Gets a list of all Piazza Services that are registered as a Task-Managed Service.
	 * 
	 * @return List of Task-Managed Services
	 */
	public List<Service> getTaskManagedServices() {
		return getServiceCollection().find().and(DBQuery.is("isTaskManaged", true)).toArray();
	}

	/**
	 * Gets the next Job in the queue for a particular service.
	 * <p>
	 * This method is synchronized because it is incredibly important that we never return the same job twice, avoiding
	 * potential race conditions.
	 * </p>
	 * 
	 * @param serviceId
	 *            The ID of the Service to fetch work for.
	 * @return The next ServiceJob in the Service Queue, if one exists; null if the Queue has no Jobs ready to be
	 *         processed.
	 */
	public synchronized ServiceJob getNextJobInServiceQueue(String serviceId) {
		// Query for Service Jobs, sort by Queue Time, so that we get the single most stale Job. Ignore Jobs that have
		// already been started. Find the latest.
		DBCursor<ServiceJob> cursor = getServiceJobCollection(serviceId).find().sort(DBSort.asc("queuedOn"))
				.and(DBQuery.is("startedOn", null)).limit(1);
		if (!cursor.hasNext()) {
			// No available Jobs to be processed.
			return null;
		}
		ServiceJob serviceJob = cursor.next();
		// Set the current Started Time that this Job was pulled off the queue
		getServiceJobCollection(serviceId).update(DBQuery.is("jobId", serviceJob.getJobId()),
				DBUpdate.set("startedOn", new DateTime().getMillis()));
		// Return the Job
		return serviceJob;
	}

	/**
	 * Gets the complete list of all Jobs in the Service's Service Queue that have exceeded the length of time for
	 * processing, and are thus considered timed out.
	 * 
	 * @param serviceId
	 *            The ID of the Service whose Queue to check
	 * @return The list of timed out Jobs
	 */
	public List<ServiceJob> getTimedOutServiceJobs(String serviceId) {
		// Get the Service details to find out the timeout information
		Service service = getServiceById(serviceId);
		Long timeout = service.getTimeout();
		if (timeout == null) {
			// If no timeout is specified for the Service, then we can't check for timeouts.
			return new ArrayList<ServiceJob>();
		}
		// The timeout is in seconds. Get the current time and subtract the number of seconds to find the timestamp of a
		// timed out service.
		long timeoutEpoch = new DateTime().minusSeconds(timeout.intValue()).getMillis();
		// Query the database for Jobs whose startedOn field is older than the timeout date.
		JacksonDBCollection<ServiceJob, String> collection = getServiceJobCollection(serviceId);
		DBCursor<ServiceJob> jobs = collection.find(DBQuery.exists("startedOn"));
		jobs = jobs.and(DBQuery.lessThan("startedOn", timeoutEpoch));
		// Return the list
		return jobs.toArray();
	}

	/**
	 * Gets the Service Job for the specified service with the specified Job ID
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param jobId
	 *            The ID of the Job
	 * @return
	 */
	public ServiceJob getServiceJob(String serviceId, String jobId) throws MongoException {
		BasicDBObject query = new BasicDBObject("jobId", jobId);
		ServiceJob serviceJob;

		try {
			serviceJob = getServiceJobCollection(serviceId).findOne(query);
		} catch (MongoTimeoutException mte) {
			String error = "Mongo Instance Not Available.";
			LOGGER.error(error, mte);
			throw new MongoException(error);
		}

		return serviceJob;
	}

	/**
	 * Increments the timeout count for the Job ID
	 * 
	 * @param serviceId
	 *            The Service ID containing the Job
	 * @param serviceJob
	 *            The ServiceJob that has timed out
	 */
	public synchronized void incrementServiceJobTimeout(String serviceId, ServiceJob serviceJob) {
		// Increment the failure count.
		getServiceJobCollection(serviceId).update(DBQuery.is("jobId", serviceJob.getJobId()),
				DBUpdate.set("timeouts", serviceJob.getTimeouts() + 1));
		// Delete the previous Started On date, so that it can be picked up again.
		getServiceJobCollection(serviceId).update(DBQuery.is("jobId", serviceJob.getJobId()), DBUpdate.unset("startedOn"));
	}

	/**
	 * Adds a new Service Job reference to the specified Service's Job Queue.
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param serviceJob
	 *            The ServiceJob, describing the ID of the Job
	 */
	public void addJobToServiceQueue(String serviceId, ServiceJob serviceJob) {
		getServiceJobCollection(serviceId).insert(serviceJob);
	}

	/**
	 * Removes the specified Service Job (by ID) from the Service Queue for the specified Service
	 * 
	 * @param serviceId
	 *            The ID of the service whose Job to remove
	 * @param jobId
	 *            The ID of the Job to remove from the queue
	 */
	public void removeJobFromServiceQueue(String serviceId, String jobId) {
		DBObject matchJob = new BasicDBObject("jobId", jobId);
		getServiceJobCollection(serviceId).remove(matchJob);
	}

	/**
	 * Gets a reference to the Service Job collection for a Service
	 */
	public JacksonDBCollection<ServiceJob, String> getServiceJobCollection(String serviceId) {
		String serviceCollectionName = getServiceQueueCollectionName(serviceId);
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(serviceCollectionName);
		return JacksonDBCollection.wrap(collection, ServiceJob.class, String.class);
	}

	/**
	 * Gets a reference to the JobManager's Jobs Collection.
	 * 
	 * @return
	 */
	public JacksonDBCollection<Job, String> getJobCollection() {
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(JOB_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Job.class, String.class);
	}

	/**
	 * Returns a Job that matches the specified Id.
	 * 
	 * @param jobId
	 *            Job Id
	 * @return The Job with the specified Id
	 * @throws InterruptedException
	 */
	public Job getJobById(String jobId) throws ResourceAccessException, InterruptedException {
		BasicDBObject query = new BasicDBObject("jobId", jobId);
		Job job;

		try {
			if ((job = getJobCollection().findOne(query)) == null) {
				// In case the Job was being updated, or it doesn't exist at this point, try once more. I admit this is
				// not optimal, but it certainly covers a host of race conditions.
				Thread.sleep(100);
				job = getJobCollection().findOne(query);
			}
		} catch (MongoTimeoutException mte) {
			LOGGER.error(mte.getMessage(), mte);
			throw new ResourceAccessException("MongoDB instance not available.");
		}

		return job;
	}

	/**
	 * Completely deletes a Service Queue for a registered service.
	 * 
	 * @param serviceId
	 *            The ID of the Service whose queue to drop.
	 */
	public void deleteServiceQueue(String serviceId) {
		getServiceJobCollection(serviceId).drop();
	}

	/**
	 * Determines if the User is able to access the Service Queue for the specified service or not.
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param username
	 *            The user name
	 * @return True if able to access, false if not.
	 */
	public boolean canUserAccessServiceQueue(String serviceId, String username) throws InvalidInputException {
		try {
			Service service = getServiceById(serviceId);
			if (service.getTaskAdministrators() != null) {
				return service.getTaskAdministrators().contains(username);
			} else {
				return false;
			}
		} catch (ResourceAccessException exception) {
			LOGGER.info(String.format("User %s attempted to check Service Queue for non-existent service with ID %s", username, serviceId),
					exception);
			throw new InvalidInputException("Service Not Found.");
		}
	}

	/**
	 * Gets the MongoDB Collection name for the list of Service Jobs for a specific Service.
	 * <p>
	 * Each Task-Managed Service will receive its own collection name based on its Service ID. By having separate
	 * collections, instead of a single collection for all Service Queues, this allows for much faster queries.
	 * </p>
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @return The Collection Name for that Service's Service Queues
	 */
	private String getServiceQueueCollectionName(String serviceId) {
		return String.format("%s-%s", SERVICE_QUEUE_COLLECTION_PREFIX, serviceId);
	}

	/**
	 * Gets Metadata on the specified Service Queue
	 * 
	 * @param serviceId
	 *            The ID of the service
	 * @return Map containing metadata information
	 */
	public Map<String, Object> getServiceQueueCollectionMetadata(String serviceId) {
		Map<String, Object> map = new HashMap<>();
		// Get the Length
		map.put("totalJobCount", getServiceJobCollection(serviceId).find().count());
		return map;
	}
}