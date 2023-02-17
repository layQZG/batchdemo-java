package com.qzg.batch.batchdemo.listener;

import com.qzg.batch.batchdemo.pojo.BlogInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;

import java.util.List;

/**
 * @author 瞿兆刚
 * @date 2021/8/23
 */
@Slf4j
public class MyWriteListener implements ItemWriteListener<BlogInfo> {

    @Override
    public void beforeWrite(List<? extends BlogInfo> items) {
    }

    @Override
    public void afterWrite(List<? extends BlogInfo> items) {
    }

    @Override
    public void onWriteError(Exception exception, List<? extends BlogInfo> items) {
        try {
            log.info("%s%n", exception.getMessage());
            for (BlogInfo message : items) {
                log.info("Failed writing BlogInfo : %s", message.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
