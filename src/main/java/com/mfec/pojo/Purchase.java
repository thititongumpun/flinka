package com.mfec.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Purchase {
    private long userId;
    private long productId;
    private double amount;
}
