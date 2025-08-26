//package com.jd.vearch.config;
//
//import com.jd.vearch.operation.BaseOperation;
//import com.jd.vearch.operation.ClusterOperation;
//import com.jd.vearch.operation.DatabaseOperation;
//import com.jd.vearch.operation.SpaceOperation;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
//import org.springframework.boot.context.properties.EnableConfigurationProperties;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@ConditionalOnClass(BaseOperation.class)
//@EnableConfigurationProperties(VearchProperties.class)
//public class VearchAutoConfiguration {
//
//    private final VearchProperties vearchProperties;
//
//    public VearchAutoConfiguration(VearchProperties vearchProperties) {
//        this.vearchProperties = vearchProperties;
//    }
//
//    @Bean
//    public ClusterOperation clusterOperation(){
//        ClusterOperation clusterOperation = new ClusterOperation();
//        clusterOperation.applyConfig(vearchProperties);
//        return clusterOperation;
//    }
//
//    @Bean
//    public DatabaseOperation databaseOperation(){
//        DatabaseOperation databaseOperation = new DatabaseOperation();
//        databaseOperation.applyConfig(vearchProperties);
//        return databaseOperation;
//    }
//
//    public SpaceOperation spaceOperation(){
//        SpaceOperation spaceOperation = new SpaceOperation();
//    }
//
//
//}
