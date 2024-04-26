package com.mfec.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class PersonInfo {
    private String name;
    private String accountName;
    private double amount;
    private String transactionType;
    @JsonProperty("piProctime")
    private String piProctime;
}
