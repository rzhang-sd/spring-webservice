package com.ml.usermapping.controller;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

/**
 * 
 * Created from Amazon kinesis sample code
 * 
 * @author raymond.zhang
 *
 */
public class KinesisStreamProducer {
	
    private static final Logger log = LoggerFactory.getLogger(KinesisStreamProducer.class);
    
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
    
    /**
     * Timestamp we'll attach to every record
     */
    private static final String TIMESTAMP = Long.toString(System.currentTimeMillis());
    
    /**
     * Change these to try larger or smaller records.
     */
    private static final int DATA_SIZE = 128;
    
    /**
     * Put records for this number of seconds before exiting.
     */
    private static final int SECONDS_TO_RUN = 5;
    
    /**
     * Put this number of records per second.
     * 
     * Because multiple logical records are combined into each Kinesis record,
     * even a single shard can handle several thousand records per second, even
     * though there is a limit of 1000 Kinesis records per shard per second.
     * 
     * If a shard gets throttled, the KPL will continue to retry records until
     * either they succeed or reach a TTL set in the KPL's configuration, at
     * which point the KPL will return failures for those records.
     * 
     * @see {@link KinesisProducerConfiguration#setRecordTtl(long)}
     */
    private static final int RECORDS_PER_SECOND = 2000;
    
    /**
     * Change this to your stream name.
     */
    public static final String STREAM_NAME = "bh-rzhang-stream";
    
    /**
     * Change this to the region you are using.
     */
    public static final String REGION = "us-west-2";
    
    /**
     * Here'll walk through some of the config options and create an instance of
     * KinesisProducer, which will be used to put records.
     * 
     * @return KinesisProducer instance used to put records.
     */
    public static KinesisProducer getKinesisProducer() {
        // There are many configurable parameters in the KPL. See the javadocs
        // on each each set method for details.
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        
        // You can also load config from file. A sample properties file is
        // included in the project folder.
        // KinesisProducerConfiguration config =
        //     KinesisProducerConfiguration.fromPropertiesFile("default_config.properties");
        
        // If you're running in EC2 and want to use the same Kinesis region as
        // the one your instance is in, you can simply leave out the region
        // configuration; the KPL will retrieve it from EC2 metadata.
        config.setRegion(REGION);
        
        // You can pass credentials programmatically through the configuration,
        // similar to the AWS SDK. DefaultAWSCredentialsProviderChain is used
        // by default, so this configuration can be omitted if that is all
        // that is needed.
        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        
        // The maxConnections parameter can be used to control the degree of
        // parallelism when making HTTP requests. We're going to use only 1 here
        // since our throughput is fairly low. Using a high number will cause a
        // bunch of broken pipe errors to show up in the logs. This is due to
        // idle connections being closed by the server. Setting this value too
        // large may also cause request timeouts if you do not have enough
        // bandwidth.
        config.setMaxConnections(1);
        
        // Set a more generous timeout in case we're on a slow connection.
        config.setRequestTimeout(60000);
        
        // RecordMaxBufferedTime controls how long records are allowed to wait
        // in the KPL's buffers before being sent. Larger values increase
        // aggregation and reduces the number of Kinesis records put, which can
        // be helpful if you're getting throttled because of the records per
        // second limit on a shard. The default value is set very low to
        // minimize propagation delay, so we'll increase it here to get more
        // aggregation.
        config.setRecordMaxBufferedTime(15000);

        // If you have built the native binary yourself, you can point the Java
        // wrapper to it with the NativeExecutable option. If you want to pass
        // environment variables to the executable, you can either use a wrapper
        // shell script, or set them for the Java process, which will then pass
        // them on to the child process.
        // config.setNativeExecutable("my_directory/kinesis_producer");
        
        // If you end up using the default configuration (a Configuration instance
        // without any calls to set*), you can just leave the config argument
        // out.
        //
        // Note that if you do pass a Configuration instance, mutating that
        // instance after initializing KinesisProducer has no effect. We do not
        // support dynamic re-configuration at the moment.
        KinesisProducer producer = new KinesisProducer(config);
        
        return producer;
    }

}
