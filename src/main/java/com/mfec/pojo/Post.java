package com.mfec.pojo;

import lombok.Data;

@Data
// @AllArgsConstructor
public class Post {
    private Integer postId;
    private String title;
    private String content;
    private Integer blogId;
}
