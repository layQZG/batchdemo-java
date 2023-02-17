package com.qzg.batch.batchdemo.processor;

import com.qzg.batch.batchdemo.pojo.BlogInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;

/**
 * @author 瞿兆刚
 * @date 2021/8/23
 */
@Slf4j
public class MyItemProcessor extends ValidatingItemProcessor<BlogInfo> {
    @Override
    public BlogInfo process(BlogInfo item) throws ValidationException {
        /**
         * 需要执行super.process(item)才会调用自定义校验器
         */
        super.process(item);
        /**
         * 对数据进行简单的处理
         */
        int i = 0;
        if (item.getBlogItem().equals("springboot")) {
            item.setBlogTitle("springbootqzg333");
            i++;
        } else {
            item.setBlogTitle("未知系列");
        }
        log.info("改变数量"+i);
        return item;
    }

}
