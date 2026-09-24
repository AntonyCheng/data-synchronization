package org.dromara.sync.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ScheduledExecutorFactoryBean;

/**
 * The sync module's own scheduler. Every {@code @Scheduled} pass of the module (task / group
 * status refresh, cron trigger, whole-database discovery, DDL check, Kafka bridge reconcile,
 * orphan sweep, alerts, metrics purge) names it with {@code scheduler = SCHEDULER}. They used
 * to share RuoYi's {@code schedule-pool} ({@code ThreadPoolConfig}, cores + 1 threads) with every
 * other module, so a pass stuck on an unreachable engine - a status refresh is up to two 10 s
 * REST calls per task / table - could starve unrelated jobs. Now it can only delay sync passes.
 *
 * <p>Registered as a {@link java.util.concurrent.ScheduledExecutorService} with
 * {@code autowireCandidate = false}, deliberately not as a {@code ThreadPoolTaskScheduler}.
 * Spring picks the scheduler of an unqualified {@code @Scheduled} method as "the only
 * TaskScheduler bean", else "the only ScheduledExecutorService bean". The application defines no
 * TaskScheduler (RuoYi only registers its ScheduledExecutorService), so a TaskScheduler here would
 * silently become the default for every module. A second, non-autowire-candidate
 * ScheduledExecutorService is skipped by that lookup and by every by-type injection of
 * ScheduledExecutorService (the push module injects RuoYi's pool that way), while
 * {@code @Scheduled(scheduler = SCHEDULER)} still resolves it by bean name.
 *
 * <p>The pool defaults to {@value #DEFAULT_POOL_SIZE} threads, one per pass: every pass is
 * {@code fixedDelay}, so it never overlaps itself and more threads can never be busy at once,
 * while fewer would let the engine-bound passes (status refreshes, cron trigger, discovery,
 * orphan sweep) starve the alerts and the bridge reconcile exactly when the engine is down.
 * Idle threads park on the delay queue.
 *
 * <p>Shutdown: on context close the executor stops triggering passes before any bean is
 * destroyed, and the lifecycle stop phase waits for an in-flight pass to finish (bounded by
 * {@code spring.lifecycle.timeout-per-shutdown-phase}) instead of interrupting it halfway through
 * a row update while the data source and Redisson are still available.
 */
@Configuration(proxyBeanMethods = false)
public class SyncSchedulingConfig {

    /** Bean name every sync {@code @Scheduled} method routes to. */
    public static final String SCHEDULER = "syncScheduler";

    /** One thread per background pass of the module; see the class comment. */
    public static final int DEFAULT_POOL_SIZE = 9;

    @Bean(name = SCHEDULER, autowireCandidate = false)
    public ScheduledExecutorFactoryBean syncScheduler(
        @Value("${sync.scheduler.pool-size:" + DEFAULT_POOL_SIZE + "}") int poolSize) {
        ScheduledExecutorFactoryBean scheduler = new ScheduledExecutorFactoryBean();
        scheduler.setPoolSize(Math.max(1, poolSize));
        scheduler.setThreadNamePrefix("sync-sched-");
        // Like RuoYi's schedule-pool: a background pass must never keep the JVM alive.
        scheduler.setDaemon(true);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }
}
