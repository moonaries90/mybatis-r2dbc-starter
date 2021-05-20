package com.lji.mybatis.r2dbc;

import com.lji.mybatis.r2dbc.base.ReactiveSqlSessionFactory;
import com.lji.mybatis.r2dbc.base.impl.DefaultReactiveSqlSessionFactory;
import com.lji.mybatis.r2dbc.conf.R2dbcConfiguration;
import com.lji.mybatis.r2dbc.conf.R2dbcMybatisProperties;
import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.executor.ErrorContext;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;

import javax.annotation.PostConstruct;

import static org.springframework.util.StringUtils.hasLength;
import static org.springframework.util.StringUtils.tokenizeToStringArray;

/**
 * r2dbc mybatis自动配置
 *
 * @author lji
 * @date 2021/05/19
 */
@Configuration
@EnableConfigurationProperties(R2dbcMybatisProperties.class)
@ConditionalOnSingleCandidate(ConnectionFactory.class)
public class R2dbcMybatisAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(R2dbcMybatisAutoConfiguration.class);

    @Bean
    public R2dbcConfiguration configuration(R2dbcMybatisProperties properties) {
        R2dbcConfiguration configuration = new R2dbcConfiguration();

        if (hasLength(properties.getTypeAliasesPackage())) {
            String[] typeAliasPackageArray = tokenizeToStringArray(properties.getTypeAliasesPackage(),
                    ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS);
            for (String packageToScan : typeAliasPackageArray) {
                configuration.getTypeAliasRegistry().registerAliases(packageToScan, Object.class);
            }
        } else {
            throw new IllegalArgumentException("typeAliasPackage cannot be empty...");
        }

        Resource[] mapperLocations = properties.resolveMapperLocations();
        if(mapperLocations != null && mapperLocations.length > 0) {
            for (Resource mapperLocation : mapperLocations) {
                if (mapperLocation == null) {
                    continue;
                }
                try {
                    XMLMapperBuilder xmlMapperBuilder = new XMLMapperBuilder(mapperLocation.getInputStream(),
                            configuration, mapperLocation.toString(), configuration.getSqlFragments());
                    xmlMapperBuilder.parse();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse mapping resource: '" + mapperLocation + "'", e);
                } finally {
                    ErrorContext.instance().reset();
                }
            }
        } else {
            throw new IllegalArgumentException("mapperLocations cannot be empty...");
        }

        return configuration;
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveSqlSessionFactory reactiveSqlSessionFactory(R2dbcConfiguration config, ConnectionFactory connectionFactory) {
        return new DefaultReactiveSqlSessionFactory(config, connectionFactory);
    }

    @org.springframework.context.annotation.Configuration
    @Import({ R2dbcAutoConfiguredMapperScannerRegistrar.class })
    @ConditionalOnMissingBean(R2dbcMapperFactoryBean.class)
    public static class MapperScannerRegistrarNotFoundConfiguration {

        @PostConstruct
        public void afterPropertiesSet() {
            logger.debug("No {} found.", MapperFactoryBean.class.getName());
        }
    }
}
