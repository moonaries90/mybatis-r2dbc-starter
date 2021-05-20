## mybatis r2dbc 自动装配

### mybatis 的实现来自 https://github.com/linux-china/mybatis-r2dbc， 新增了 pojo 的一些实现

> 测试可以用 dev.miku 包测试（不过性能不太好， 如果有 vertx-mysql-client 转 r2dbc 的实现就好了）

### 使用方式
```java
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@R2dbcMapperScan(basePackages = "com.lji.r2dbc.mapper")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
    
    @Bean
    public ConnectionFactory connectionFactory() {
        ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbc:mysql://lji:lsm@unix.cn:3306/xxl_job");
        ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration.builder(connectionFactory)
                .initialSize(200)
                .maxSize(200)
                .maxCreateConnectionTime(Duration.ZERO)
                .build();
        return new ConnectionPool(poolConfiguration);
    }
}
```

```yaml
r2dbc:
  mybatis:
    type-aliases-package: com.lji.r2dbc.meta
    mapper-locations: classpath:mapper/*.xml
```
