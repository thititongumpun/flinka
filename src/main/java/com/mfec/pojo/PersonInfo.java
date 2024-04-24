package com.mfec.pojo;

import lombok.Data;

@Data
public class PersonInfo {
    private String name;
    private String accountName;
    private double amount;
    private String transactionType;
}
