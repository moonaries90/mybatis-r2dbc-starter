package com.lji.mybatis.r2dbc;

import com.lji.mybatis.r2dbc.base.ReactiveSqlSession;
import com.lji.mybatis.r2dbc.base.ReactiveSqlSessionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import static org.springframework.util.Assert.notNull;

/**
 * r2dbc mapper工厂bean
 *
 * @author lji
 * @date 2021/05/19
 */
public class R2dbcMapperFactoryBean<T> implements FactoryBean<T>, InitializingBean {

    private Class<T> mapperInterface;

    private ReactiveSqlSession sqlSession;

    private final Logger logger = LoggerFactory.getLogger(R2dbcMapperFactoryBean.class);

    public R2dbcMapperFactoryBean() {

    }

    public R2dbcMapperFactoryBean(Class<T> clazz) {
        this.mapperInterface = clazz;
    }

    @Override
    public T getObject() throws Exception {
        return sqlSession.getMapper(this.mapperInterface);
    }

    @Override
    public Class<?> getObjectType() {
        return mapperInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setMapperInterface(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    public void setSqlSessionFactory(ReactiveSqlSessionFactory sqlSessionFactory) {
        this.sqlSession = sqlSessionFactory.openSession();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        notNull(this.sqlSession, "Property 'sqlSessionFactory' are required...");
        if (!sqlSession.getConfiguration().hasMapper(this.mapperInterface)) {
            try {
                sqlSession.getConfiguration().addMapper(this.mapperInterface);
            } catch (Exception e) {
                logger.error("Error while adding the mapper '" + this.mapperInterface + "' to configuration.", e);
                throw new IllegalArgumentException(e);
            } finally {
                ErrorContext.instance().reset();
            }
        }
    }
}
