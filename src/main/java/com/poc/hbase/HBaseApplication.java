package com.poc.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class HBaseApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(HBaseApplication.class, args);
		//new SHCTest().readData();
		new Demo().displayData();
    }

}
