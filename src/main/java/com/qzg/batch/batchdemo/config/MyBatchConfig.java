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
 * @author ?????????
 * @date 2021/8/23
 */
@Slf4j
@Configuration
@EnableBatchProcessing
public class MyBatchConfig {


    /**
     * JobRepository?????????Job???????????????????????????????????????????????????????????????
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
     * jobLauncher????????? job????????????,???????????????jobRepository
     * @param dataSource
     * @param transactionManager
     * @return
     * @throws Exception
     */
    @Bean
    public SimpleJobLauncher myJobLauncher(DataSource dataSource, PlatformTransactionManager transactionManager) throws Exception{
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        // ??????jobRepository
        jobLauncher.setJobRepository(myJobRepository(dataSource, transactionManager));
        return jobLauncher;
    }

    /**
     * ??????job
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
     * ??????job?????????
     * @return
     */
    @Bean
    public MyJobListener myJobListener(){
        return new MyJobListener();
    }

    /**
     * ItemReader???????????????????????????+entirty???????????????
     * @return
     */
    @Bean
    public ItemReader<BlogInfo> reader(){
        // ??????FlatFileItemReader??????cvs??????????????????????????????
        FlatFileItemReader<BlogInfo> reader = new FlatFileItemReader<>();
        // ????????????????????????
        reader.setResource(new ClassPathResource("static/bloginfo.csv"));
        // entity???csv???????????????
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
            //???????????????
            reader.setDataSource(dataSource);
            //??????????????????????????????
            reader.setFetchSize(200);
            //????????????????????????user??????
            reader.setRowMapper(new BeanPropertyRowMapper<>(BlogInfo.class));
            //????????????sql??????
            MySqlPagingQueryProvider provider = new MySqlPagingQueryProvider();
            provider.setSelectClause("id,blogAuthor,blogUrl,blogTitle,blogItem");
            provider.setFromClause("from bloginfo");
            //??????????????????????????????
            Map<String, Order> sort = new LinkedHashMap(1);
            //??????ID??????
            sort.put("id", Order.ASCENDING);
            provider.setSortKeys(sort);
            reader.setQueryProvider(provider);
        }catch (Exception e){
            log.error("????????????",e);
            return reader;
        }
        return reader;
    }
    /**
     * ???????????????
     * @return
     */
    @Bean
    public MyBeanValidator myBeanValidator(){
        return new MyBeanValidator<BlogInfo>();
    }

    /**
     * ??????ItemProcessor: ????????????+????????????
     * @return
     */
    @Bean
    public ItemProcessor<BlogInfo, BlogInfo> processor(){
        MyItemProcessor myItemProcessor = new MyItemProcessor();
        // ???????????????
        myItemProcessor.setValidator(myBeanValidator());
        return myItemProcessor;
    }

    /**
     * ItemWriter???????????????datasource?????????????????????sql????????????????????????
     * @param dataSource
     * @return
     */
    @Bean
    public ItemWriter<BlogInfo> writer(DataSource dataSource){
        // ??????jdbcBcatchItemWrite????????????????????????
        JdbcBatchItemWriter<BlogInfo> writer = new JdbcBatchItemWriter<>();
        // ??????????????????sql??????
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<BlogInfo>());
        String sql = "insert into bloginfo "+" (blogAuthor,blogUrl,blogTitle,blogItem) "
                +" values(:blogAuthor,:blogUrl,:blogTitle,:blogItem)";
        writer.setSql(sql);
        writer.setDataSource(dataSource);
        return writer;
    }

    /**
     * step?????????
     * ??????
     * ItemReader ??????
     * ItemProcessor  ??????
     * ItemWriter ??????
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
                .<BlogInfo, BlogInfo>chunk(101) // Chunk?????????(????????????????????????????????????????????????????????????????????????????????????????????????writer??????????????????)
                .reader(fromdbReader).faultTolerant().retryLimit(3).retry(Exception.class).skip(Exception.class).skipLimit(2)
                .listener(new MyReadListener())
                .processor(processor)
                .writer(writer).faultTolerant().skip(Exception.class).skipLimit(2)
                .listener(new MyWriteListener())
                .build();
    }


}
