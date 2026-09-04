package org.dromara.config;

import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.Schedules;

import java.lang.reflect.Method;

/**
 * 仅在 {@code dev-fast} profile 下生效。
 * <p>
 * {@code application-dev-fast.yml} 打开了 {@code spring.main.lazy-initialization}，
 * 而 Spring 在延迟初始化模式下不会为尚未创建的 bean 注册 {@code @Scheduled} 任务。
 * 数据同步平台的 cron 调度、整库表发现、DDL 差异检查都依赖 {@code @Scheduled}，
 * 因此这里把所有带 {@code @Scheduled}/{@code @Schedules} 方法的 bean 排除在延迟初始化之外，
 * 让它们在启动时照常创建、照常注册定时任务。
 *
 * @author data-sync
 */
@Profile("dev-fast")
@Configuration
public class DevFastLazyInitConfig {

    /**
     * 让所有包含定时任务方法的 bean 在延迟初始化模式下仍然 eager 创建。
     *
     * @return 排除过滤器
     */
    @Bean
    static LazyInitializationExcludeFilter scheduledBeansLazyInitExcludeFilter() {
        return (beanName, beanDefinition, beanType) -> {
            for (Method method : beanType.getMethods()) {
                if (method.isAnnotationPresent(Scheduled.class) || method.isAnnotationPresent(Schedules.class)) {
                    return true;
                }
            }
            return false;
        };
    }
}
