package com.ml.usermapping.controller;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.ml.usermapping.domain.BHCEPMapping;
import com.ml.usermapping.domain.Response;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;

@RestController
public class UserController {
	private static final String mappingTable = "apikey-cep-mapping";
	private static final String keyInMappingTb = "BHAPIKey";
	
	private static final int DATA_SIZE = 128;
	// my kinesis stream name
	private static final String STREAM_NAME = "bh-rzhang-stream";
	private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());
	
	/**
     * Put records for this number of seconds before exiting.
     */
    private static final int SECONDS_TO_RUN = 5;
    
    private static final int RECORDS_PER_SECOND = 2000;
	
	private static final Logger log = LoggerFactory.getLogger(UserController.class);
	
	private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

	@ApiOperation(value = "Get list of tables avaiable in the mapping service", notes = "")
	@RequestMapping(value = "/tables", method = RequestMethod.GET)
	public List<String> getTables() {

		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
		List<String> tables = new ArrayList<String>();
		try {
			ListTablesResult table_list = null;
			table_list = ddb.listTables();
			List<String> table_names = table_list.getTableNames();

			if (table_names.size() > 0) {
				for (String table_name : table_names) {
					System.out.format("* %s\n", table_name);
					tables.add(table_name);
				}
			}

		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		}
		return tables;
	}
	
	@ApiOperation(value = "Kinesis feeds consumer", notes = "")
	@RequestMapping(value = "/consumer", method = RequestMethod.GET)
	public ResponseEntity<Response> consumeFromStream() {
		KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(
                        "KinesisProducerLibSampleConsumer",
                        KinesisStreamProducer.STREAM_NAME,
                        new DefaultAWSCredentialsProviderChain(),
                        "KinesisProducerLibSampleConsumer")
                                .withRegionName(KinesisStreamProducer.REGION)
                                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        
        final KinesisStreamConsumer consumer = new KinesisStreamConsumer();
        
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                consumer.logResults();
            }
        }, 10, 1, TimeUnit.SECONDS);
        
        new Worker.Builder()
            .recordProcessorFactory(consumer)
            .config(config)
            .build()
            .run();
        
        Response rsp = new Response(null, "read kinesis feeds");
		return new ResponseEntity<Response>(rsp, HttpStatus.OK);
	}
	
	@ApiOperation(value = "Kinesis feeds producer", notes = "")
	@RequestMapping(value = "/producer", method = RequestMethod.GET)
	public ResponseEntity<Response> injestToStream() {
		
		KinesisProducer kinesis = KinesisStreamProducer.getKinesisProducer();
		
		// The monotonically increasing sequence number we will put in the data of each record
        final AtomicLong sequenceNumber = new AtomicLong(0);
        
        // The number of records that have finished (either successfully put, or failed)
        final AtomicLong completed = new AtomicLong(0);
        
        // KinesisProducer.addUserRecord is asynchronous. A callback can be used to receive the results.
        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                // We don't expect any failures during this sample. If it
                // happens, we will log the first one and exit.
                if (t instanceof UserRecordFailedException) {
                    Attempt last = Iterables.getLast(
                            ((UserRecordFailedException) t).getResult().getAttempts());
                    log.error(String.format(
                            "Record failed to put - %s : %s",
                            last.getErrorCode(), last.getErrorMessage()));
                    System.err.println("record failed to put");
                }
                log.error("Exception during put", t);
                System.err.println("Exception during put");
                System.exit(1);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
            	System.err.println("Put record succeed");
                completed.getAndIncrement();
            }
        };
        
        // The lines within run() are the essence of the KPL API.
        final Runnable putOneRecord = new Runnable() {
        	       	
            @Override
            public void run() {
            	// Set Test data to the kinesis stream
            	StringBuilder sb = new StringBuilder();
                sb.append("711014");
                sb.append(" ");
                // 
                while (sb.length() < DATA_SIZE) {
                    sb.append("a");
                }
                
                ByteBuffer data = null;
				try {
					data = ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                
                // TIMESTAMP is our partition key
                String hashKey = new BigInteger(128, new Random()).toString(10);
                ListenableFuture<UserRecordResult> f =
                		kinesis.addUserRecord(STREAM_NAME, TIMESTAMP, hashKey, data);
                Futures.addCallback(f, callback);
            }
        };
        
        // Kick off the puts
        log.info(String.format(
                "Starting puts... will run for %d seconds at %d records per second",
                SECONDS_TO_RUN, RECORDS_PER_SECOND));
        
        executeAtTargetRate(EXECUTOR, putOneRecord, sequenceNumber, SECONDS_TO_RUN, RECORDS_PER_SECOND);
        
        // Wait for puts to finish. After this statement returns, we have
        // finished all calls to putRecord, but the records may still be
        // in-flight. We will additionally wait for all records to actually
        // finish later.
        try {
			EXECUTOR.awaitTermination(SECONDS_TO_RUN + 1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // If you need to shutdown your application, call flushSync() first to
        // send any buffered records. This method will block until all records
        // have finished (either success or fail). There are also asynchronous
        // flush methods available.
        //
        // Records are also automatically flushed by the KPL after a while based
        // on the time limit set with Configuration.setRecordMaxBufferedTime()
        log.info("Waiting for remaining puts to finish...");
        kinesis.flushSync();
        log.info("All records complete.");
        
        // This kills the child process and shuts down the threads managing it.
        kinesis.destroy();
        log.info("Finished.");
        
        
		Response rsp = new Response(null, "sent kinesis feeds");
		return new ResponseEntity<Response>(rsp, HttpStatus.OK);
	}
	
	private static void executeAtTargetRate(
            final ScheduledExecutorService exec,
            final Runnable task,
            final AtomicLong counter,
            final int durationSeconds,
            final int ratePerSecond) {
        exec.scheduleWithFixedDelay(new Runnable() {
            final long startTime = System.nanoTime();

            @Override
            public void run() {
                double secondsRun = (System.nanoTime() - startTime) / 1e9;
                double targetCount = Math.min(durationSeconds, secondsRun) * ratePerSecond;
                
                while (counter.get() < targetCount) {
                    counter.getAndIncrement();
                    try {
                        task.run();
                    } catch (Exception e) {
                        log.error("Error running task", e);
                        System.exit(1);
                    }
                }
                
                if (secondsRun >= durationSeconds) {
                    exec.shutdown();
                }
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }
	

	@ApiOperation(value = "Get User's info by BH API Key", notes = "Query for the user by BH API Key")
	@ApiImplicitParam(name = "bhkey", value = "BH API Key", required = true, dataType = "String", paramType = "path")
	@RequestMapping(value = "/users/{bhkey}", method = RequestMethod.GET)
	public ResponseEntity<Response> get(@PathVariable(value = "bhkey") String bhapikey) {
		return getByKey(bhapikey);
	}

	/**
	 * Include following in client request's header
	 * 
	 * Content-Type: application/json;charset=UTF-8 Accept: application/json
	 * 
	 * @param paramMap
	 * @return
	 */
	@ApiOperation(value = "Add new user's info", notes = "Insert a new user mapping")
	@RequestMapping(value = "/users", method = RequestMethod.POST, consumes = {
			MediaType.APPLICATION_FORM_URLENCODED_VALUE, MediaType.APPLICATION_JSON_VALUE }, produces = {
					MediaType.APPLICATION_ATOM_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<Response> add(@RequestBody BHCEPMapping paramMap) {

		try {
			String encryptedPS = encryptPws(paramMap.getcEPPassword());
			paramMap.setcEPPassword(encryptedPS);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		HashMap<String, AttributeValue> item_values = paramMap.toMapForAdd();
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

		try {
			ddb.putItem(mappingTable, item_values);
		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The table \"%s\" can't be found.\n", mappingTable);
			Response rsp = new Response(null, e.getErrorMessage());
			return new ResponseEntity<Response>(rsp, HttpStatus.NOT_FOUND);
		} catch (AmazonServiceException e) {
			System.err.println(e.getMessage());
			Response rsp = new Response(null, e.getErrorMessage());
			return new ResponseEntity<Response>(rsp, HttpStatus.INTERNAL_SERVER_ERROR);
		}

		HttpHeaders headers = new HttpHeaders();
		URI location = ServletUriComponentsBuilder.fromCurrentServletMapping().path("/users/{key}").build()
				.expand(paramMap.getbHApiKey()).toUri();

		headers.setLocation(location);
		Response rsp = new Response(null, "Added user to Database");
		return new ResponseEntity<Response>(rsp, HttpStatus.CREATED);
	}

	/**
	 * Include following in client request's header
	 * 
	 * Content-Type: application/json;charset=UTF-8 Accept: application/json
	 * 
	 * @param paramMap
	 * @return
	 */
	@ApiOperation(value = "Update a user's info", notes = "Update an existing user mapping")
	@RequestMapping(value = "/users", method = RequestMethod.PUT, consumes = { MediaType.APPLICATION_JSON_VALUE,
			MediaType.APPLICATION_FORM_URLENCODED_VALUE }, produces = { MediaType.APPLICATION_ATOM_XML_VALUE,
					MediaType.APPLICATION_JSON_VALUE })
	public ResponseEntity<Response> update(@RequestBody BHCEPMapping paramMap) {

		HashMap<String, AttributeValueUpdate> updatedValues = paramMap.toMapForUpdate();

		Map<String, AttributeValue> itemKey = new HashMap<String, AttributeValue>();
		itemKey.put(keyInMappingTb, new AttributeValue(paramMap.getbHApiKey()));

		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

		try {
			ddb.updateItem(mappingTable, itemKey, updatedValues);
		} catch (ResourceNotFoundException e) {
			System.err.println(e.getMessage());
			Response rsp = new Response(null, e.getErrorMessage());
			return new ResponseEntity<Response>(rsp, HttpStatus.NOT_FOUND);
		} catch (AmazonServiceException e) {
			System.err.println(e.getMessage());
			Response rsp = new Response(null, e.getErrorMessage());
			return new ResponseEntity<Response>(rsp, HttpStatus.INTERNAL_SERVER_ERROR);
		}

		// update succeed, return the updated resource
		return getByKey(paramMap.getbHApiKey());
	}

	private ResponseEntity<Response> getByKey(String bhapikey) {
		Map<String, AttributeValue> keysToGet = new HashMap<String, AttributeValue>();
		keysToGet.put(keyInMappingTb, new AttributeValue(bhapikey));

		GetItemRequest request = new GetItemRequest().withKey(keysToGet).withTableName(mappingTable);

		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

		BHCEPMapping mapping = new BHCEPMapping();

		try {
			Map<String, AttributeValue> returned_item = ddb.getItem(request).getItem();
			if (returned_item != null) {
				Set<String> keys = returned_item.keySet();
				for (String key : keys) {
					System.out.format("%s: %s\n", key, returned_item.get(key).toString());
					// decrypt password
					if (key == "CEPPassword") {
						try {
							// String encryptedPS =
							// returned_item.get(key).toString();
							String encryptedPS = "Ro7v1H6EC0Pryesqc8ny9A==";
							String decryptedPS = decryptPws(encryptedPS);
							mapping.set(BHCEPMapping.ToFields.get(key), decryptedPS);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else {
						mapping.set(BHCEPMapping.ToFields.get(key), returned_item.get(key).toString());
					}
				}
			} else {
				System.out.format("No item found with the key %s!\n", bhapikey);
				HttpHeaders headers = new HttpHeaders();
				return new ResponseEntity<Response>(headers, HttpStatus.NOT_FOUND);
			}
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			Response rsp = new Response(null, e.getErrorMessage());
			return new ResponseEntity<Response>(rsp, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		Response rsp = new Response(mapping, "");
		return new ResponseEntity<Response>(rsp, HttpStatus.OK);
	}

	@ApiOperation(value = "Remove a user's info by BH API Key", notes = "Remove user's info if BH API Key match")
	@ApiImplicitParam(name = "bhkey", value = "BH API Key", required = true, dataType = "String", paramType = "path")
	@RequestMapping(value = "/users/{bhkey}", method = RequestMethod.DELETE)
	public ResponseEntity<Response> delete(@PathVariable(value = "bhkey") String bhapikey) {

		HashMap<String, AttributeValue> keysToGet = new HashMap<String, AttributeValue>();
		keysToGet.put(keyInMappingTb, new AttributeValue(bhapikey));

		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();

		try {
			ddb.deleteItem(mappingTable, keysToGet);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			Response rsp = new Response(null, e.getErrorMessage());
			return new ResponseEntity<Response>(rsp, HttpStatus.INTERNAL_SERVER_ERROR);
		}
		Response rsp = new Response(null, "Record deleted");
		return new ResponseEntity<Response>(rsp, HttpStatus.OK);
	}

	@ApiOperation(value = "API Info", notes = "")
	@RequestMapping("/info")
	public String getInfo(@RequestParam(value = "bhkey", defaultValue = "bhapi-key") String name) {
		return "BHAPI-CEP-AUTH 1.0";
	}

	public String encryptPws(String plainText) throws Exception {
		String secretKey = "TestSecretKey111";
		SecretKeySpec key = new SecretKeySpec(secretKey.getBytes(), "AES");
		Cipher cipher = Cipher.getInstance("AES");
		cipher.init(Cipher.ENCRYPT_MODE, key);
		byte[] encryptedByte = cipher.doFinal(plainText.getBytes());
		Base64.Encoder encoder = Base64.getEncoder();
		String encryptedText = encoder.encodeToString(encryptedByte);
		return encryptedText;
	}

	public String decryptPws(String encryptText) throws Exception {

		String secretKey = "TestSecretKey111";
		SecretKeySpec key = new SecretKeySpec(secretKey.getBytes(), "AES");
		Cipher cipher = Cipher.getInstance("AES");
		cipher.init(Cipher.DECRYPT_MODE, key);
		Base64.Decoder decoder = Base64.getDecoder();
		byte[] encryptedTextByte = decoder.decode(encryptText);
		byte[] decryptedByte = cipher.doFinal(encryptedTextByte);
		String decryptedText = new String(decryptedByte);
		return decryptedText;
	}
}
