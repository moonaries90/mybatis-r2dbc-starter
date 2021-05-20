package com.lji.mybatis.r2dbc.base.impl;

import com.lji.mybatis.r2dbc.base.ReactiveSqlSession;
import com.lji.mybatis.r2dbc.base.type.R2DBCTypeHandler;
import com.lji.mybatis.r2dbc.base.type.RowWrapper;
import com.lji.mybatis.r2dbc.base.type.UnMappedColumnHandler;
import com.lji.mybatis.r2dbc.conf.R2dbcConfiguration;
import io.r2dbc.spi.*;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeException;
import org.apache.ibatis.type.TypeHandler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

/**
 * reactive sql session default implementation
 *
 * @author linux_china
 */
@SuppressWarnings("unchecked")
public class DefaultReactiveSqlSession implements ReactiveSqlSession {
    private final List<Class<?>> NUMBER_TYPES = Arrays.asList(byte.class, short.class, int.class, long.class, float.class, double.class,
            Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class);
    private final R2dbcConfiguration configuration;
    private final ObjectFactory objectFactory;
    private final ConnectionFactory connectionFactory;
    private final boolean metricsEnabled;

    public DefaultReactiveSqlSession(R2dbcConfiguration configuration, ConnectionFactory connectionFactory) {
        this.configuration = configuration;
        this.objectFactory = this.configuration.getObjectFactory();
        //noinspection
        this.connectionFactory = connectionFactory;
        //metrics enabled
        this.metricsEnabled = Boolean.parseBoolean(configuration.getVariables().getProperty("metrics.enabled", "false"));
    }

    @Override
    public <T> Mono<T> selectOne(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Mono<T> rowSelected = getConnection().flatMap(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            ResultMap resultMap = mappedStatement.getResultMaps().get(0);
            return executeFluxStatement(connection, statement)
                    .flatMap(result -> result.map((row, rowMetadata) -> (T) convertRowToResult(row, rowMetadata, resultMap)))
                    .last();
        });
        if (metricsEnabled) {
            return rowSelected.name(statementId).metrics();
        } else {
            return rowSelected;
        }
    }

    @Override
    public <T> Flux<T> select(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Flux<T> rowsSelected = getConnection().flatMapMany(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            ResultMap resultMap = mappedStatement.getResultMaps().get(0);
            return executeFluxStatement(connection, statement)
                    .flatMap(result -> result.map((row, rowMetadata) -> (T) convertRowToResult(row, rowMetadata, resultMap)));
        });
        if (metricsEnabled) {
            return rowsSelected.name(statementId).metrics();
        } else {
            return rowsSelected;
        }
    }

    @Override
    public <T> Flux<T> select(String statementId, Object parameter, RowBounds rowBounds) {
        return (Flux<T>) select(statementId, parameter).skip(rowBounds.getOffset()).limitRequest(rowBounds.getLimit());
    }

    @Override
    public Mono<Integer> insert(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Mono<Integer> rowsUpdated = getConnection().flatMap(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            final boolean useGeneratedKeys = mappedStatement.getKeyGenerator() != null && mappedStatement.getKeyProperties() != null;
            if (useGeneratedKeys) {
                statement.returnGeneratedValues(mappedStatement.getKeyProperties());
            }
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            return executeMonoStatement(connection, statement)
                    .flatMap(result -> {
                        if (!useGeneratedKeys) {
                            return Mono.from(result.getRowsUpdated());
                        } else {
                            return Mono.from(result.map((row, rowMetadata) -> {
                                MetaObject parameterMetaObject = configuration.newMetaObject(parameter);
                                for (String keyProperty : mappedStatement.getKeyProperties()) {
                                    Object value = row.get(keyProperty, parameterMetaObject.getSetterType(keyProperty));
                                    parameterMetaObject.setValue(keyProperty, value);
                                }
                                return 1;
                            }));
                        }
                    });
        });
        if (metricsEnabled) {
            return rowsUpdated.name(statementId).metrics();
        } else {
            return rowsUpdated;
        }
    }

    @Override
    public Mono<Integer> delete(String statementId, Object parameter) {
        return update(statementId, parameter);
    }

    @Override
    public Mono<Integer> update(String statementId, Object parameter) {
        MappedStatement mappedStatement = configuration.getMappedStatement(statementId);
        BoundSql boundSql = mappedStatement.getBoundSql(parameter);
        Mono<Integer> updatedRows = getConnection().flatMap(connection -> {
            Statement statement = connection.createStatement(boundSql.getSql());
            if (parameter != null) {
                fillParams(statement, boundSql, parameter);
            }
            return executeMonoStatement(connection, statement)
                    .flatMap(result -> Mono.from(result.getRowsUpdated()));
        });
        if (metricsEnabled) {
            return updatedRows.name(statementId).metrics();
        } else {
            return updatedRows;
        }
    }

    @Override
    public <T> T getMapper(Class<T> clazz) {
        return configuration.getMapper(clazz, this);
    }

    @Override
    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void fillParams(Statement statement, BoundSql boundSql, Object parameter) {
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        if (parameterMappings != null) {
            for (int i = 0; i < parameterMappings.size(); i++) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                if (parameterMapping.getMode() != ParameterMode.OUT) {
                    Object value;
                    String propertyName = parameterMapping.getProperty();
                    if (boundSql.hasAdditionalParameter(propertyName)) {
                        value = boundSql.getAdditionalParameter(propertyName);
                    } else if (parameter == null) {
                        value = null;
                    } else if (configuration.getR2dbcTypeHandlerRegistry().hasTypeHandler(parameter.getClass())) {
                        value = parameter;
                    } else {
                        MetaObject metaObject = configuration.newMetaObject(parameter);
                        value = metaObject.getValue(propertyName);
                    }
                    if (value == null) {
                        statement.bindNull(i, parameterMapping.getJavaType());
                        return;
                    }
                    TypeHandler<?> typeHandler = parameterMapping.getTypeHandler();
                    try {
                        Class<?> parameterClass = value.getClass();
                        if (typeHandler instanceof R2DBCTypeHandler) {
                            ((R2DBCTypeHandler<Object>) typeHandler).setParameter(statement, i, value, parameterMapping.getJdbcType());
                        } else if (configuration.getR2dbcTypeHandlerRegistry().hasTypeHandler(parameterClass)) {
                            configuration.getR2dbcTypeHandlerRegistry().getTypeHandler(parameterClass).setParameter(statement, i, value, parameterMapping.getJdbcType());
                        } else {
                            statement.bind(i, value);
                        }
                    } catch (TypeException e) {
                        throw new TypeException("Could not set parameters for mapping: " + parameterMapping + ". Cause: " + e, e);
                    }
                }
            }
        }
    }

    public Object convertRowToResult(Row row, RowMetadata rowMetadata, ResultMap resultMap) {
        //number
        Class<?> type = resultMap.getType();
        if (NUMBER_TYPES.contains(type)) {
            Number columnValue = (Number) row.get(0);
            if (columnValue == null) {
                return null;
            }
            if (type.equals(columnValue.getClass())) {
                return columnValue;
            } else if (type.equals(Byte.class) || type.equals(byte.class)) {
                return columnValue.byteValue();
            } else if (type.equals(Short.class) || type.equals(short.class)) {
                return columnValue.shortValue();
            } else if (type.equals(Integer.class) || type.equals(int.class)) {
                return columnValue.intValue();
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                return columnValue.longValue();
            } else if (type.equals(Float.class) || type.equals(float.class)) {
                return columnValue.floatValue();
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                return columnValue.doubleValue();
            } else {
                return columnValue;
            }
        } else if (configuration.getR2dbcTypeHandlerRegistry().hasTypeHandler(type)) {
            R2DBCTypeHandler<?> mappingTypeHandler = configuration.getR2dbcTypeHandlerRegistry().getTypeHandler(type);
            return mappingTypeHandler.getResult(row, 0, rowMetadata);
        } else if (type.isAssignableFrom(Map.class)) {
            Map<String, Object> result = new HashMap<>();
            for (String columnName : rowMetadata.getColumnNames()) {
                result.put(columnName, row.get(columnName));
            }
            return result;
        } else if (type.isAssignableFrom(Collection.class)) {
            List<Object> result = new ArrayList<>();
            for (String columnName : rowMetadata.getColumnNames()) {
                result.add(row.get(columnName));
            }
            return result;
        } else {
            Object object = objectFactory.create(type);
            MetaObject resultMetaObject = configuration.newMetaObject(object);
            List<ResultMapping> resultMappings = resultMap.getResultMappings();
            if (!resultMappings.isEmpty()) {
                for (ResultMapping resultMapping : resultMappings) {
                    Class<?> javaType = resultMapping.getJavaType();
                    Object columnValue;
                    TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
                    if (typeHandler instanceof R2DBCTypeHandler) {
                        columnValue = ((R2DBCTypeHandler<?>) typeHandler).getResult(row, resultMapping.getColumn(), rowMetadata);
                    } else if (configuration.getR2dbcTypeHandlerRegistry().hasTypeHandler(javaType)) {
                        columnValue = configuration.getR2dbcTypeHandlerRegistry().getTypeHandler(javaType).getResult(row, resultMapping.getColumn(), rowMetadata);
                    } else {
                        columnValue = row.get(resultMapping.getColumn(), javaType);
                    }
                    resultMetaObject.setValue(resultMapping.getProperty(), columnValue);
                }
            } else {
                RowWrapper rsw = new RowWrapper(row, rowMetadata, configuration);
                List<UnMappedColumnHandler.UnMappedColumnAutoMapping> unmappedResultMappings = UnMappedColumnHandler.createAutomaticMappings(rsw, resultMap, resultMetaObject);
                if(unmappedResultMappings.size() > 0) {
                    for(UnMappedColumnHandler.UnMappedColumnAutoMapping mapping : unmappedResultMappings) {
                        final Object value = mapping.getTypeHandler().getResult(rsw.getRow(), mapping.getColumn(), rowMetadata);
                        if (value != null || (configuration.isCallSettersOnNulls() && !mapping.isPrimitive())) {
                            resultMetaObject.setValue(mapping.getProperty(), value);
                        }
                    }
                } else {
                    rowMetadata.getColumnNames().forEach(column -> {
                        Object columnValue = row.get(column);
                        resultMetaObject.setValue(column, columnValue);
                    });
                }
            }
            return object;
        }
    }

    private Flux<? extends Result> executeFluxStatement(Connection connection, Statement statement) {
        return Flux.from(statement.execute())
                .doFinally(a -> ((Mono<Void>) connection.close()).subscribe());
    }
    private Mono<? extends Result> executeMonoStatement(Connection connection, Statement statement) {
        return Mono.from(statement.execute())
                .doFinally(a -> ((Mono<Void>) connection.close()).subscribe());
    }
    private Mono<Connection> getConnection() {
        return (Mono<Connection>) connectionFactory.create();
    }

}
