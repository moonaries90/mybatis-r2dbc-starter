package com.lji.mybatis.r2dbc.conf;

import com.lji.mybatis.r2dbc.base.ReactiveSqlSession;
import com.lji.mybatis.r2dbc.base.type.R2DBCTypeHandler;
import com.lji.mybatis.r2dbc.base.type.TypeHandlerRegistry;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.TypeHandler;
import org.springframework.beans.factory.InitializingBean;

/**
 * r2dbc配置
 *
 * @author lji
 * @date 2021/05/19
 */
public class R2dbcConfiguration extends Configuration implements InitializingBean {

    private final R2dbcMapperRegistry mapperRegistry = new R2dbcMapperRegistry(this);

    private final TypeHandlerRegistry typeHandlerRegistry = new TypeHandlerRegistry();

    public <T> T getMapper(Class<T> type, ReactiveSqlSession session) {
        return mapperRegistry.getMapper(type, session);
    }

    @Override
    public <T> void addMapper(Class<T> type) {
        mapperRegistry.addMapper(type);
    }

    @Override
    public void addMappers(String packageName, Class<?> superType) {
        mapperRegistry.addMappers(packageName, superType);
    }

    @Override
    public void addMappers(String packageName) {
        mapperRegistry.addMappers(packageName);
    }

    @Override
    public boolean hasMapper(Class<?> type) {
        return mapperRegistry.hasMapper(type);
    }

    public TypeHandlerRegistry getR2dbcTypeHandlerRegistry() {
        return typeHandlerRegistry;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (TypeHandler<?> typeHandler : super.getTypeHandlerRegistry().getTypeHandlers()) {
            if (typeHandler instanceof R2DBCTypeHandler) {
                R2DBCTypeHandler<?> r2DBCTypeHandler = (R2DBCTypeHandler<?>) typeHandler;
                typeHandlerRegistry.register(r2DBCTypeHandler);
            }
        }
    }
}
