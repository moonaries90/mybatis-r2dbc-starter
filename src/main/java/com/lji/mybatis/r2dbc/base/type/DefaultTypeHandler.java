package com.lji.mybatis.r2dbc.base.type;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.apache.ibatis.type.JdbcType;

/**
 * 默认类型处理
 *
 * @author lji
 * @date 2021/05/19
 */
public class DefaultTypeHandler<T> implements R2DBCTypeHandler<T> {

    private final Class<T> clazz;

    public DefaultTypeHandler(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void setParameter(Statement statement, int i, T parameter, JdbcType jdbcType) throws R2dbcException {
        if(parameter != null) {
            statement.bind(i, parameter);
        } else {
            statement.bindNull(i, clazz);
        }
    }

    @Override
    public T getResult(Row row, String columnName, RowMetadata rowMetadata) throws R2dbcException {
        return row.get(columnName, clazz);
    }

    @Override
    public T getResult(Row row, int columnIndex, RowMetadata rowMetadata) throws R2dbcException {
        return row.get(columnIndex, clazz);
    }

    @Override
    public Class<?> getType() {
        return clazz;
    }
}
