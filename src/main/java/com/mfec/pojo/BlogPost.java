package com.mfec.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BlogPost {
    private Integer blogId;
    private String url;
    private Integer postId;
    private String title;
    private String content;
}
