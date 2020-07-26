package com.boot;


import com.alibaba.fastjson.JSON;
import com.onlyone.delayqueue.t2.CacheService;
import com.onlyone.delayqueue.t2.T2Start;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = T2Start.class)
public class CacheServiceTest {


    @Resource
    private CacheService cacheService;

    String keyPrefix = "delayqueue";

    @Test
    public void test() throws InterruptedException {

        //清理数据
        cacheService.delKey(keyPrefix);

        // 模拟插入10条记录
        for (int i = 1; i <= 10; i++) {
            long delayTime = Instant.now().plusSeconds(i + 4).getEpochSecond();
            boolean result = cacheService.addData(keyPrefix, "v=" + i, delayTime);
            if (result) {
                System.out.println("记录：" + i + " 插入成功！");
            }
        }

        // 启动延迟队列扫描
        while (true) {
            long nowtTime = Instant.now().getEpochSecond();
            // 一次扫描出小于当前时间且按时间排序的最小两条记录
            List<String> result = cacheService.scanData(keyPrefix, nowtTime, 2);
            if (result != null) {
                for (String record : result) {
                    //redis单线程机制解决并发场景安全问题
                    cacheService.removeData(keyPrefix, record);
                    System.out.println("扫描出来的超时记录：" + JSON.toJSONString(result));

                }
            }
            Thread.sleep(800);
        }

    }


}
