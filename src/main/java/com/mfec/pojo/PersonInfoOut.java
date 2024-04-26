package com.mfec.pojo;

import lombok.Data;

@Data
public class PersonInfoOut {
    private String name;
    private String jobTitle;
    private String accountName;
    private double amount;
    private String transactionType;
    private String proctime;
}
