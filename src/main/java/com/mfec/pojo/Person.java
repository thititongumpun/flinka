package com.mfec.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Person {
    private String name;
    private String jobTitle;
    @JsonProperty("pProctime")
    private String pProctime;
}
