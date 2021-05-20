package com.lji.mybatis.r2dbc.base.impl;

import com.lji.mybatis.r2dbc.base.ReactiveSqlSession;
import com.lji.mybatis.r2dbc.base.ReactiveSqlSessionFactory;
import com.lji.mybatis.r2dbc.conf.R2dbcConfiguration;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.session.Configuration;

import java.io.IOException;

/**
 * Default ReactiveSqlSessionFactory
 *
 * @author linux_china
 */
public class DefaultReactiveSqlSessionFactory implements ReactiveSqlSessionFactory {

    private final R2dbcConfiguration configuration;

    private final ConnectionFactory connectionFactory;

    private final ReactiveSqlSession sqlSession;

    public DefaultReactiveSqlSessionFactory(R2dbcConfiguration configuration, ConnectionFactory connectionFactory) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.sqlSession = new DefaultReactiveSqlSession(configuration, this.connectionFactory);
    }

    @Override
    public ReactiveSqlSession openSession() {
        return this.sqlSession;
    }

    @Override
    public Configuration getConfiguration() {
        return this.configuration;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return this.connectionFactory;
    }

    @Override
    public void close() throws IOException {
        if (this.connectionFactory instanceof ConnectionPool) {
            ConnectionPool connectionPool = ((ConnectionPool) this.connectionFactory);
            if (!connectionPool.isDisposed()) {
                connectionPool.dispose();
            }
        }
    }
}
