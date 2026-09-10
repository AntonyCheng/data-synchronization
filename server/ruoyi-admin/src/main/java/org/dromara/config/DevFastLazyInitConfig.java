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
 * {@code application-dev-fast.yml} 打开了 {@code spring.main.lazy-initialization}。延迟初始化
 * 会推迟一批 bean 的创建，其中有两类如果不在启动时创建就会破坏功能，这里逐一排除：
 * <ol>
 *   <li>{@code @Scheduled}/{@code @Schedules} bean —— Spring 不会为尚未创建的 bean 注册定时任务，
 *       而 cron 调度、整库表发现、DDL 差异检查都依赖它。</li>
 *   <li>Sa-Token 的装配 bean（{@code cn.dev33.satoken.spring.*}）—— {@code SaBeanInject} 通过
 *       {@code @Autowired} setter 把项目自定义的 {@link cn.dev33.satoken.stp.StpLogic}
 *       （{@code StpLogicJwtForSimple}，JWT 模式）注入全局 {@code StpUtil}。若它被延迟，
 *       首次 {@code /auth/login} 会用 Sa-Token 默认的 {@code StpLogicForType} 签发一个
 *       随机 UUID token；等到某个带鉴权注解的请求触发 {@code SaBeanInject} 创建后，全局逻辑
 *       才切换为 JWT，之前签发的 UUID token 随即失效，前端表现为登录后立刻
 *       “登录状态异常，请重新登录”。让装配 bean 保持 eager 即可在任何登录前完成 StpLogic 切换。</li>
 * </ol>
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

    /**
     * 让 Sa-Token 的装配 bean 在延迟初始化模式下仍然 eager 创建，确保自定义 StpLogic（JWT）
     * 在第一个登录请求之前就已生效。{@code SaBeanInject} 通过构造参数与 setter 依赖
     * {@code SaTokenConfig}、{@code StpLogic}、{@code SaTokenDao}、{@code StpInterface}，
     * 让它 eager 会连带把这些依赖一并提前创建。
     *
     * @return 排除过滤器
     */
    @Bean
    static LazyInitializationExcludeFilter saTokenWiringLazyInitExcludeFilter() {
        return (beanName, beanDefinition, beanType) -> beanType.getName().startsWith("cn.dev33.satoken.spring.");
    }
}
