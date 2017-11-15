package com.poc.hbase.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Created by hduser on 13/11/17.
 */

@Configuration
public class BaseConfiguration {

    private static Logger logger = LoggerFactory.getLogger(BaseConfiguration.class);

    @Bean
    public Connection configuration() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "localhost:60000");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.zookeeper.quorum", "localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        logger.info("Configuration created");
        return ConnectionFactory.createConnection(configuration);
    }

}
