package com.qzg.batch.batchdemo.config;

import com.qzg.batch.batchdemo.listener.MyJobListener;
import com.qzg.batch.batchdemo.listener.MyReadListener;
import com.qzg.batch.batchdemo.listener.MyWriteListener;
import com.qzg.batch.batchdemo.pojo.BlogInfo;
import com.qzg.batch.batchdemo.pojo.MyBeanValidator;
import com.qzg.batch.batchdemo.processor.MyItemProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author 瞿兆刚
 * @date 2021/8/23
 */
@Slf4j
@Configuration
@EnableBatchProcessing
public class MyBatchConfig {


    /**
     * JobRepository定义：Job的注册容器以及和数据库打交道（事务管理等）
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public JobRepository myJobRepository(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception{
        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDatabaseType("mysql");
        jobRepositoryFactoryBean.setTransactionManager(transactionManager);
        jobRepositoryFactoryBean.setDataSource(dataSource);
        return jobRepositoryFactoryBean.getObject();
    }


    /**
     * jobLauncher定义： job的启动器,绑定相关的jobRepository
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public SimpleJobLauncher myJobLauncher(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception{
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        // 设置jobRepository
        jobLauncher.setJobRepository(myJobRepository(dataSource, transactionManager));
        return jobLauncher;
    }

    /**
     * 定义job
     * @param jobs
     * @param myStep
     * @return
     */
    @Bean
    public Job myJob(JobBuilderFactory jobs, Step myStep){
        return jobs.get("myJob")
                .incrementer(new RunIdIncrementer())
                .flow(myStep)
                .end()
                .listener(myJobListener())
                .build();
    }

    /**
     * 注册job监听器
     * @return
     */
    @Bean
    public MyJobListener myJobListener(){
        return new MyJobListener();
    }

    /**
     * ItemReader定义：读取文件数据+entirty实体类映射
     * @return
     */
    @Bean
    public ItemReader<BlogInfo> reader(){
        // 使用FlatFileItemReader去读cvs文件，一行即一条数据
        FlatFileItemReader<BlogInfo> reader = new FlatFileItemReader<>();
        // 设置文件处在路径
        reader.setResource(new ClassPathResource("static/bloginfo.csv"));
        // entity与csv数据做映射
        reader.setLineMapper(new DefaultLineMapper<BlogInfo>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[]{"id","blogAuthor","blogUrl","blogTitle","blogItem"});
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<BlogInfo>() {
                    {
                        setTargetType(BlogInfo.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    public ItemReader<BlogInfo> fromdbReader(DataSource dataSource) {
        JdbcPagingItemReader<BlogInfo>  reader = null;
        try {
            reader = new JdbcPagingItemReader<>();
            //注入数据源
            reader.setDataSource(dataSource);
            //设置一次读取几条数据
            reader.setFetchSize(200);
            //将读取的记录转成user对象
            reader.setRowMapper(new BeanPropertyRowMapper<>(BlogInfo.class));
            //如何指定sql语句
            MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();
            provider.setSelectClause("id,blogAuthor,blogUrl,blogTitle,blogItem");
            provider.setFromClause("from bloginfo");
            //如何根据哪个字段排序
            Map<String, Order> sort = new LinkedHashMap(1);
            //根据ID升序
            sort.put("id", Order.ASCENDING);
            provider.setSortKeys(sort);
            reader.setQueryProvider(provider);
        }catch (Exception e){
            log.error("读取异常",e);
            return reader;
        }
        return reader;
    }
    /**
     * 注册校验器
     * @return
     */
    @Bean
    public MyBeanValidator myBeanValidator(){
        return new MyBeanValidator<BlogInfo>();
    }

    /**
     * 注册ItemProcessor: 处理数据+校验数据
     * @return
     */
    @Bean
    public ItemProcessor<BlogInfo, BlogInfo> processor(){
        MyItemProcessor myItemProcessor = new MyItemProcessor();
        // 设置校验器
        myItemProcessor.setValidator(myBeanValidator());
        return myItemProcessor;
    }

    /**
     * ItemWriter定义：指定datasource，设置批量插入sql语句，写入数据库
     * @param dataSource
     * @return
     */
    @Bean
    public ItemWriter<BlogInfo> writer(DataSource dataSource){
        // 使用jdbcBcatchItemWrite写数据到数据库中
        JdbcBatchItemWriter<BlogInfo> writer = new JdbcBatchItemWriter<>();
        // 设置有参数的sql语句
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<BlogInfo>());
        String sql = "insert into bloginfo "+" (blogAuthor,blogUrl,blogTitle,blogItem) "
                +" values(:blogAuthor,:blogUrl,:blogTitle,:blogItem)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;
    }

    /**
     * step定义：
     * 包括
     * ItemReader 读取
     * ItemProcessor  处理
     * ItemWriter 输出
     * @param stepBuilderFactory
     * @param fromdbReader
     * @param writer
     * @param processor
     * @return
     */

    @Bean
    public Step myStep(StepBuilderFactory stepBuilderFactory, ItemReader<BlogInfo> fromdbReader,
                       ItemWriter<BlogInfo> writer, ItemProcessor<BlogInfo, BlogInfo> processor){
        return stepBuilderFactory
                .get("myStep")
                .<BlogInfo, BlogInfo>chunk(101) // Chunk的机制(即每次读取一条数据，再处理一条数据，累积到一定数量后再一次性交给writer进行写入操作)
                .reader(fromdbReader).faultTolerant().retryLimit(3).retry(Exception.class).skip(Exception.class).skipLimit(2)
                .listener(new MyReadListener())
                .processor(processor)
                .writer(writer).faultTolerant().skip(Exception.class).skipLimit(2)
                .listener(new MyWriteListener())
                .build();
    }


}
