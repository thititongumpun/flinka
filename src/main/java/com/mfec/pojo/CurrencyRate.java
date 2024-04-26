package com.mfec.pojo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class CurrencyRate {
    @JsonProperty("update_time")
    private String updateTime;
    private String currency;
    private Integer rate;
    @JsonProperty("proctime")
    private String proctime;
}
