package org.venice.piazza.servicecontroller.messaging;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.mongodb.accessors.MongoAccessor;
import org.venice.piazza.servicecontroller.messaging.handlers.DeleteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.DescribeServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ExecuteServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.ListServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.RegisterServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.SearchServiceHandler;
import org.venice.piazza.servicecontroller.messaging.handlers.UpdateServiceHandler;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;
import org.venice.piazza.servicecontroller.util.CoreUUIDGen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.DataType;
import model.data.type.BodyDataType;
import model.data.type.RasterDataType;
import model.data.type.TextDataType;
import model.data.type.URLParameterDataType;
import model.job.Job;
import model.job.PiazzaJobType;
import model.job.result.type.DataResult;
import model.job.type.ExecuteServiceJob;
import model.job.type.IngestJob;
import model.request.PiazzaJobRequest;
import model.service.metadata.ExecuteServiceData;
import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import util.PiazzaLogger;
import util.UUIDFactory;

@Component
public class ServiceMessageThreadManager {
	// Jobs to listen to
		@Value("${space}")
		private String space;
		private final String DELETE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "delete-service", space);
		private final String EXECUTE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "execute-service", space);
		private final String READ_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "read-service", space);
		private final String REGISTER_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "register-service", space);
		private final String UPDATE_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "update-service", space);
		private final String LIST_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "list-service", space);
		private final String SEARCH_SERVICE_JOB_TOPIC_NAME = String.format("%s-%s", "search-service", space);

		private final static Logger LOGGER = LoggerFactory.getLogger(ServiceMessageThreadManager.class);
		
		private String KAFKA_HOST;

		private String KAFKA_PORT;
		private String KAFKA_GROUP;

		
		/*
		  TODO need to determine how statuses will be sent to update the job  (Call back?)
		 */
		private Producer<String, String> producer;
		private Consumer<String, String> consumer;
		private List<String> topics = Arrays.asList(DELETE_SERVICE_JOB_TOPIC_NAME, EXECUTE_SERVICE_JOB_TOPIC_NAME, 
				   READ_SERVICE_JOB_TOPIC_NAME, REGISTER_SERVICE_JOB_TOPIC_NAME,
				   UPDATE_SERVICE_JOB_TOPIC_NAME,LIST_SERVICE_JOB_TOPIC_NAME, SEARCH_SERVICE_JOB_TOPIC_NAME);;
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private RegisterServiceHandler rsHandler;
		private ExecuteServiceHandler esHandler;
		private UpdateServiceHandler usHandler;
		private DescribeServiceHandler dsHandler;
		private DeleteServiceHandler dlHandler;
		private ListServiceHandler lsHandler;
		private SearchServiceHandler ssHandler;
		
		
		private ThreadPoolExecutor executor;
		private Map<String, Future<?>> runningServiceRequests;

		@Autowired
		private MongoAccessor accessor;
		@Autowired
		private CoreServiceProperties coreServiceProperties;
		
		@Autowired
		private PiazzaLogger coreLogger;
		
		@Autowired
		private UUIDFactory uuidFactory;


		/**
		 * Constructor for ServiceMessageThreadManager
		 */
		public ServiceMessageThreadManager() { 

		}

		/**+
		 * 
		 */
		@PostConstruct
		public void initialize() {
			// Initialize the Kafka consumer/producer

			String kafkaHostFull = coreServiceProperties.getKafkaHost();
			KAFKA_GROUP = coreServiceProperties.getKafkaGroup();
			
			KAFKA_HOST = kafkaHostFull.split(":")[0];
			KAFKA_PORT = kafkaHostFull.split(":")[1];
			
			LOGGER.info("=================================");

			LOGGER.info("The KAFKA Host Properties is " + coreServiceProperties.getKafkaHost());
			LOGGER.info("The KAFKA Group Properties is " + coreServiceProperties.getKafkaGroup());
			LOGGER.info("=================================");


			/* Initialize producer and consumer for the Kafka Queue */

			producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
			consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);

			// Initialize the HashMap
			runningServiceRequests = new HashMap<String, Future<?>>();

			// Initialize the ThreadPoolManager
			executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
			
			// Start polling for Kafka Jobs on the Group Consumer.. This occurs on a
			// separate Thread so as not to block Spring.
			Thread kafkaListenerThread = new Thread() {
				public void run() {
					pollServiceJobs();
				}
			};
			// Subscribe for the topics
			consumer.subscribe(topics);
			kafkaListenerThread.start();
			
			// Start polling for Kafka Abort Jobs on the unique Consumer.
			Thread pollAbortThread = new Thread() {
				public void run() {
					pollAbortServiceJobs();
				}
			};
			pollAbortThread.start();
		}
	   /**
	    * Polls for service controller topics and handles these requests.
	    */
		public void pollServiceJobs() {
			
			ObjectMapper mapper = new ObjectMapper();
			try {
				Job job = null;
			
				// Create a Callback that will be invoked when a Worker completes. This will
				WorkerCallback callback = new WorkerCallback() {
					@Override
					public void onComplete(String jobId) {
						runningServiceRequests.remove(jobId);
					}
				};
				while (!closed.get()) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
					// Handle new Messages on this topic.
					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						LOGGER.info("Received topic: " + consumerRecord.topic() + " with key "
								+ consumerRecord.key());
						//coreLogger.log("Received topic: " + consumerRecord.topic() + " with key "
						//		+ consumerRecord.key(), coreLogger.INFO);
												
						// Wrap the JobRequest in the Job object
						try {
							job = mapper.readValue(consumerRecord.value(), Job.class);	
							
							if (job != null) {
								PiazzaJobType jobType = job.getJobType();


								ServiceMessageWorker serviceMessageWorker = new ServiceMessageWorker(consumerRecord, producer, accessor,  
															callback,coreServiceProperties, uuidFactory, coreLogger, job, space);
	
	
								Future<?> workerFuture = executor.submit(serviceMessageWorker);
	
								// Keep track of all Running Jobs
								runningServiceRequests.put(consumerRecord.key(), workerFuture);
								
							}

						} catch (Exception ex) {
							
						}
					}// for loop
				}// while loop
			} catch (Exception ex) {
				//coreLogger.log(String.format("There was a problem.", ex.getMessage()),
				//		PiazzaLogger.FATAL);
				
			}
			
		}
		
		/**
		 * Begins listening for Abort Jobs. If a Job is owned by this component,
		 * then it will be terminated.
		 */
		public void pollAbortServiceJobs() {
			try {
				// Create the Unique Consumer
				
				Consumer<String, String> uniqueConsumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT,

						String.format("%s-%s", KAFKA_GROUP, UUID.randomUUID().toString()));
				uniqueConsumer.subscribe(Arrays.asList(String.format("%s-%s", JobMessageFactory.ABORT_JOB_TOPIC_NAME, space)));

				// Poll
				while (!closed.get()) {
					ConsumerRecords<String, String> consumerRecords = uniqueConsumer.poll(1000);
					// Handle new Messages on this topic.
					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						// Determine if this Job ID is being processed by this
						// component.
						String jobId = consumerRecord.key();
						if (runningServiceRequests.containsKey(jobId)) {
							// Cancel the Running Job
							runningServiceRequests.get(jobId).cancel(true);
							// Remove it from the list of Running Jobs
							runningServiceRequests.remove(jobId);
						}
					}
				}
			} catch (WakeupException ex) {
				coreLogger.log(String.format("Polling Thread forcefully closed: %s", ex.getMessage()),
						PiazzaLogger.FATAL);
			}
		}
	
		
		
			
		
}
