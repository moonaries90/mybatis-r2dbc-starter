package com.lji.mybatis.r2dbc.base.type;

import com.lji.mybatis.r2dbc.conf.R2dbcConfiguration;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.reflection.MetaObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 未映射列处理
 *
 * @author lji
 * @date 2021/05/19
 */
public class UnMappedColumnHandler {

    private static final Map<String, List<UnMappedColumnAutoMapping>> autoMappingsCache = new ConcurrentHashMap<>();

    public static List<UnMappedColumnAutoMapping> createAutomaticMappings(RowWrapper rsw, ResultMap resultMap, MetaObject metaObject) {

        R2dbcConfiguration configuration = rsw.getConfiguration();
        TypeHandlerRegistry typeHandlerRegistry = configuration.getR2dbcTypeHandlerRegistry();

        List<UnMappedColumnAutoMapping> autoMapping = autoMappingsCache.computeIfAbsent(resultMap.getId(), (key) -> {
            List<UnMappedColumnAutoMapping> result = new ArrayList<>();
            final List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap);
            for (String columnName : unmappedColumnNames) {
                final String property = metaObject.findProperty(columnName, configuration.isMapUnderscoreToCamelCase());
                if (property != null && metaObject.hasSetter(property)) {
                    if (resultMap.getMappedProperties().contains(property)) {
                        continue;
                    }
                    final Class<?> propertyType = metaObject.getSetterType(property);
                    if (typeHandlerRegistry.hasTypeHandler(propertyType)) {
                        final R2DBCTypeHandler<?> typeHandler = rsw.getTypeHandler(propertyType, columnName);
                        result.add(new UnMappedColumnAutoMapping(columnName, property, typeHandler, propertyType.isPrimitive()));
                    } else {
                        result.add(new UnMappedColumnAutoMapping(columnName, property, new DefaultTypeHandler<>(propertyType), propertyType.isPrimitive()));
                    }
                }
            }
            return result;
        });
        autoMappingsCache.putIfAbsent(resultMap.getId(), autoMapping);
        return autoMapping;
    }

    public static class UnMappedColumnAutoMapping {

        private final String column;

        private final String property;

        private final R2DBCTypeHandler<?> typeHandler;

        private final boolean primitive;

        public UnMappedColumnAutoMapping(String column, String property, R2DBCTypeHandler<?> typeHandler, boolean primitive) {
            this.column = column;
            this.property = property;
            this.typeHandler = typeHandler;
            this.primitive = primitive;
        }

        public String getColumn() {
            return column;
        }

        public String getProperty() {
            return property;
        }

        public R2DBCTypeHandler<?> getTypeHandler() {
            return typeHandler;
        }

        public boolean isPrimitive() {
            return primitive;
        }
    }
}
