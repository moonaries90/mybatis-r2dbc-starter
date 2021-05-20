package com.lji.mybatis.r2dbc.base.binding;


import com.lji.mybatis.r2dbc.base.ReactiveSqlSession;
import com.lji.mybatis.r2dbc.base.ReactiveSqlSessionFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Lasse Voss
 * @author linux_china
 */
public class MapperProxyFactory<T> {

    private final Class<T> mapperInterface;

    private ReactiveSqlSessionFactory sessionFactory;

    private final Map<Method, MapperProxy.MapperMethodInvoker> methodCache = new ConcurrentHashMap<>();

    public MapperProxyFactory(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    public Class<T> getMapperInterface() {
        return mapperInterface;
    }

    public Map<Method, MapperProxy.MapperMethodInvoker> getMethodCache() {
        return methodCache;
    }

    @SuppressWarnings("unchecked")
    protected T newInstance(MapperProxy<T> mapperProxy) {
        return (T) Proxy.newProxyInstance(mapperInterface.getClassLoader(), new Class[]{mapperInterface}, mapperProxy);
    }

    public T newInstance(ReactiveSqlSession sqlSession) {
        final MapperProxy<T> mapperProxy = new MapperProxy<T>(sqlSession, mapperInterface, methodCache);
        return newInstance(mapperProxy);
    }

    public ReactiveSqlSessionFactory getSessionFactory() {
        return sessionFactory;
    }

    public void setSessionFactory(ReactiveSqlSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }
}
