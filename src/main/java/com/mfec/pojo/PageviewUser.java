package com.mfec.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PageviewUser {
    private long viewtime;
    private String userid;
    private String pageid;
    private long registertime;
    private String regionid;
    private String gender;
}
