package com.lji.mybatis.r2dbc.base;

import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.session.Configuration;

import java.io.Closeable;

/**
 * reactive SQL session factory
 *
 * @author linux_china
 */
public interface ReactiveSqlSessionFactory extends Closeable {

    ReactiveSqlSession openSession();

    Configuration getConfiguration();

    ConnectionFactory getConnectionFactory();
}
