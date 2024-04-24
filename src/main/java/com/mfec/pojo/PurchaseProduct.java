package com.mfec.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PurchaseProduct {
    private int purchase_id;
    private String item_type;
    private int quantity;
    private double price_per_unit;
    private int product_id;
    private String name;
    private String description;
    private int price;
}
