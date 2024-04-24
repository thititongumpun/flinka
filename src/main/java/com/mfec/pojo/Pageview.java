package com.mfec.pojo;

import lombok.Data;

@Data
public class Pageview {
    private long viewtime;
    private String userid;
    private String pageid;
}
