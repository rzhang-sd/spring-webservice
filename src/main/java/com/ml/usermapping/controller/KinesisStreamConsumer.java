package com.ml.usermapping.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class KinesisStreamConsumer implements IRecordProcessorFactory {
	
    private static final Logger log = LoggerFactory.getLogger(KinesisStreamConsumer.class);
    
    // All records from a run of the producer have the same timestamp in their
    // partition keys. Since this value increases for each run, we can use it
    // determine which run is the latest and disregard data from earlier runs.
    private final AtomicLong largestTimestamp = new AtomicLong(0);
    
    // List of record sequence numbers we have seen so far.
    private final List<Long> sequenceNumbers = new ArrayList<>();
    
    // A mutex for largestTimestamp and sequenceNumbers. largestTimestamp is
    // nevertheless an AtomicLong because we cannot capture non-final variables
    // in the child class.
    private final Object lock = new Object();
	
	/**
     * One instance of RecordProcessor is created for every shard in the stream.
     * All instances of RecordProcessor share state by capturing variables from
     * the enclosing SampleConsumer instance. This is a simple way to combine
     * the data from multiple shards.
     */
    private class RecordProcessor implements IRecordProcessor {
        @Override
        public void initialize(String shardId) {}

        @Override
        public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
            long timestamp = 0;
            List<Long> seqNos = new ArrayList<>();
            
            for (Record r : records) {
                // Get the timestamp of this run from the partition key.
                timestamp = Math.max(timestamp, Long.parseLong(r.getPartitionKey()));
                
                // Extract the sequence number. It's encoded as a decimal
                // string and placed at the beginning of the record data,
                // followed by a space. The rest of the record data is padding
                // that we will simply discard.
                try {
                    byte[] b = new byte[r.getData().remaining()]; 
                    System.out.println("Read Data: " + new String(b));
                    r.getData().get(b);
                    seqNos.add(Long.parseLong(new String(b, "UTF-8").split(" ")[0]));
                } catch (Exception e) {
                    log.error("Error parsing record", e);
                    System.exit(1);
                }
            }
            
            synchronized (lock) {
                if (largestTimestamp.get() < timestamp) {
                    log.info(String.format(
                            "Found new larger timestamp: %d (was %d), clearing state",
                            timestamp, largestTimestamp.get()));
                    largestTimestamp.set(timestamp);
                    sequenceNumbers.clear();
                }
                
                // Only add to the shared list if our data is from the latest run.
                if (largestTimestamp.get() == timestamp) {
                    sequenceNumbers.addAll(seqNos);
                    Collections.sort(sequenceNumbers);
                }
            }
            
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                log.error("Error while trying to checkpoint during ProcessRecords", e);
            }
        }

        @Override
        public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
            log.info("Shutting down, reason: " + reason);
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                log.error("Error while trying to checkpoint during Shutdown", e);
            }
        }
    }
    
    /**
     * Log a message indicating the current state.
     */
    public void logResults() {
        synchronized (lock) {
            if (largestTimestamp.get() == 0) {
                return;
            }
            
            if (sequenceNumbers.size() == 0) {
                log.info("No sequence numbers found for current run.");
                return;
            }
            
            // The producer assigns sequence numbers starting from 1, so we
            // start counting from one before that, i.e. 0.
            long last = 0;
            long gaps = 0;
            for (long sn : sequenceNumbers) {
                if (sn - last > 1) {
                    gaps++;
                }
                last = sn;
            }
            
            log.info(String.format(
                    "Found %d gaps in the sequence numbers. Lowest seen so far is %d, highest is %d",
                    gaps, sequenceNumbers.get(0), sequenceNumbers.get(sequenceNumbers.size() - 1)));
        }
    }

	@Override
	public IRecordProcessor createProcessor() {
		return this.new RecordProcessor();
	}

}
