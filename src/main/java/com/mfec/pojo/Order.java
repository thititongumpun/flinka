package com.mfec.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Order {
    @JsonProperty("order_time")
    private String orderTime;
    private Integer amount;
    private String currency;
    @JsonProperty("proctime")
    private String proctime;
}
