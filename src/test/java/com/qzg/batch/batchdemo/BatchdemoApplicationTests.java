package com.qzg.batch.batchdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.deploy.util.StringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

@SpringBootTest
class BatchdemoApplicationTests {

    @Test
    void contextLoads() throws IOException {
        FileInputStream fis=new FileInputStream("C:\\Users\\Administrator\\Desktop\\撞库.fin");

        InputStreamReader isr=new InputStreamReader(fis, "UTF-8");
        BufferedReader br = new BufferedReader(isr);

//简写如下

//BufferedReader br = new BufferedReader(new InputStreamReader(

//        new FileInputStream("E:/phsftp/evdokey/evdokey_201103221556.txt"), "UTF-8"));

        String line="";

        String[] arrs=null;

        while ((line=br.readLine())!=null) {


        }

        br.close();

        isr.close();

        fis.close();

    }

}
