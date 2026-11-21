package com.projects.distibutedraftkvstore;

import org.springframework.boot.SpringApplication;

public class TestDistibutedRaftKvStoreApplication {

    public static void main(String[] args) {
        SpringApplication.from(DistibutedRaftKvStoreApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
