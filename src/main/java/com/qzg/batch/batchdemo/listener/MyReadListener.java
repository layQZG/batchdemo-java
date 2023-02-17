package com.qzg.batch.batchdemo.listener;

import com.qzg.batch.batchdemo.pojo.BlogInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemReadListener;

/**
 * @author 瞿兆刚
 * @date 2021/8/23
 */
@Slf4j
public class MyReadListener implements ItemReadListener<BlogInfo> {

    @Override
    public void beforeRead() {
    }

    @Override
    public void afterRead(BlogInfo item) {
    }

    @Override
    public void onReadError(Exception ex) {
        try {
            log.info("读取的异常："+ ex);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
