package com.acme.kafka.connect.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.time.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceTask;

import static com.acme.kafka.connect.sample.SampleSourceConnectorConfig.*;

public class SampleSourceTask extends SourceTask {
    private static Logger log = LoggerFactory.getLogger(SampleSourceTask.class);

    private SampleSourceConnectorConfig config;
    private int monitorThreadTimeout;
    private List<String> sources;

    @Override
    public String version() {
        return PropertiesUtil.getConnectorVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new SampleSourceConnectorConfig(properties);
        monitorThreadTimeout = config.getInt(MONITOR_THREAD_TIMEOUT_CONFIG);
        String sourcesStr = properties.get("sources");
        sources = Arrays.asList(sourcesStr.split(","));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(monitorThreadTimeout / 2);
        List<SourceRecord> records = new ArrayList<>();
        SourceRecord record;
        for (String source : sources) {
            record = createSourceRecordWithHeaders(source);
            // log.info("Polling data from the source '" + source + "' " + record.value());
            log.info("Record prepared: " + record.value());
            records.add(record);
        }
        // log.info("Processed " + sources.size() + " source records");
        return records;
    }

    private SourceRecord createSourceRecord(String source) {
        Long timestamp = System.currentTimeMillis();
        return new SourceRecord(Collections.singletonMap("source", source), Collections.singletonMap("offset", 0),
                source, null, null, null, Schema.BYTES_SCHEMA,
                String.format("Data from %s [%d]", source, timestamp).getBytes());
    }

    private SourceRecord createSourceRecordWithHeaders(String source) {
        Long timestamp = System.currentTimeMillis();
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("X-CUSTOM-HEADER", String.format("Send %d", timestamp));
        return new SourceRecord(Collections.singletonMap("source", source), Collections.singletonMap("offset", 0),
                source, null, null, null, Schema.BYTES_SCHEMA,
                String.format("Data from %s [%d]", source, timestamp).getBytes(), timestamp, headers);
    }

    @Override
    public void stop() {
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        log.info("Record commited: " + record.value());
        super.commitRecord(record, metadata);
    }

}
