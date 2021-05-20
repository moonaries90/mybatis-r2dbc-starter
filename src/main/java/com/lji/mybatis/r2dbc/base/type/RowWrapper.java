package com.lji.mybatis.r2dbc.base.type;

import com.lji.mybatis.r2dbc.conf.R2dbcConfiguration;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.apache.ibatis.mapping.ResultMap;
import java.util.*;

/**
 * 行包装
 *
 * @author lji
 * @date 2021/05/19
 */
public class RowWrapper {

    private final Row row;

    private final R2dbcConfiguration configuration;

    private final List<String> columnNames = new ArrayList<>();

    private final List<Class<?>> classTypes = new ArrayList<>();

    private final Map<String, Map<Class<?>, R2DBCTypeHandler<?>>> typeHandlerMap = new HashMap<>();

    private final Map<String, List<String>> unMappedColumnNamesMap = new HashMap<>();

    public RowWrapper(Row row, RowMetadata metadata, R2dbcConfiguration configuration) {
        this.row = row;
        this.configuration = configuration;
        columnNames.addAll(metadata.getColumnNames());
        for(String column : columnNames) {
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(column);
            classTypes.add(columnMetadata.getJavaType());
        }
    }

    public Row getRow() {
        return row;
    }

    public R2dbcConfiguration getConfiguration() {
        return configuration;
    }

    public List<String> getUnmappedColumnNames(ResultMap resultMap) {
        List<String> unMappedColumnNames = unMappedColumnNamesMap.get(resultMap.getId());
        if (unMappedColumnNames == null) {
            loadMappedAndUnmappedColumnNames(resultMap);
            unMappedColumnNames = unMappedColumnNamesMap.get(resultMap.getId());
        }
        return unMappedColumnNames;
    }

    private void loadMappedAndUnmappedColumnNames(ResultMap resultMap) {
        List<String> unmappedColumnNames = new ArrayList<>();
        final Set<String> mappedColumns = resultMap.getMappedColumns();
        for (String columnName : columnNames) {
            final String upperColumnName = columnName.toUpperCase(Locale.ENGLISH);
            if (!mappedColumns.contains(upperColumnName)) {
                unmappedColumnNames.add(columnName);
            }
        }
        unMappedColumnNamesMap.put(resultMap.getId(), unmappedColumnNames);
    }

    public R2DBCTypeHandler<?> getTypeHandler(Class<?> propertyType, String columnName) {
        R2DBCTypeHandler<?> handler = null;
        Map<Class<?>, R2DBCTypeHandler<?>> columnHandlers = typeHandlerMap.get(columnName);
        if (columnHandlers == null) {
            columnHandlers = new HashMap<>();
            typeHandlerMap.put(columnName, columnHandlers);
        } else {
            handler = columnHandlers.get(propertyType);
        }
        if (handler == null) {
            handler = configuration.getR2dbcTypeHandlerRegistry().getTypeHandler(propertyType);
            // Replicate logic of UnknownTypeHandler#resolveTypeHandler
            // See issue #59 comment 10
            if (handler == null) {
                final int index = columnNames.indexOf(columnName);
                final Class<?> javaType = classTypes.get(index);
                handler = configuration.getR2dbcTypeHandlerRegistry().getTypeHandler(javaType);
            }
            if (handler == null) {
                handler = new DefaultTypeHandler<>(propertyType);
            }
            columnHandlers.put(propertyType, handler);
        }
        return handler;
    }

}
