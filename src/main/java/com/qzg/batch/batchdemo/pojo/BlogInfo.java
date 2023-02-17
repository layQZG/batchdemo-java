package com.qzg.batch.batchdemo.pojo;

import lombok.Data;

/**
 * @author 瞿兆刚
 * @date 2021/8/23
 */
@Data
public class BlogInfo {
    private Integer id;
    private String blogAuthor;
    private String blogUrl;
    private String blogTitle;
    private String blogItem;
}
