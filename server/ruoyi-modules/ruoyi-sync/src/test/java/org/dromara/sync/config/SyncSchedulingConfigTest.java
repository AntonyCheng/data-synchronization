package org.dromara.sync.config;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.LazyInitializationExcludeFilter;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real Spring scheduling infrastructure against a stand-in for RuoYi's
 * {@code ThreadPoolConfig#scheduledExecutorService}, the only scheduler the application had.
 */
@Tag("dev")
class SyncSchedulingConfigTest {

    @Test
    void syncPassesRunOnTheModuleSchedulerWhileOtherModulesKeepTheFrameworkPool() throws Exception {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(FrameworkPool.class, SyncSchedulingConfig.class, Passes.class);
            context.refresh();

            Passes passes = context.getBean(Passes.class);
            assertTrue(passes.syncRan.await(5, TimeUnit.SECONDS));
            assertTrue(passes.otherRan.await(5, TimeUnit.SECONDS));
            assertTrue(passes.syncThread.startsWith("sync-sched-"), passes.syncThread);
            assertTrue(passes.otherThread.startsWith("schedule-pool-"), passes.otherThread);
            // By-type lookups (the push module injects the framework pool this way) are unaffected.
            assertSame(context.getBean("scheduledExecutorService"), context.getBean(ScheduledExecutorService.class));
        }
    }

    /** dev-fast: {@code spring.main.lazy-initialization=true} with only {@code @Scheduled} beans kept eager. */
    @Test
    void passesStillFireUnderTheDevFastLazyInitialization() throws Exception {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.addBeanFactoryPostProcessor(new LazyInitializationBeanFactoryPostProcessor());
            context.register(FrameworkPool.class, SyncSchedulingConfig.class, Passes.class, DevFastRules.class);
            context.refresh();

            // The scheduler itself is not exempted from lazy init - the passes pull it in by name.
            assertTrue(context.getBeanFactory().getBeanDefinition(SyncSchedulingConfig.SCHEDULER).isLazyInit());
            Passes passes = context.getBean(Passes.class);
            assertTrue(passes.syncRan.await(5, TimeUnit.SECONDS));
            assertTrue(passes.otherRan.await(5, TimeUnit.SECONDS));
            assertTrue(passes.syncThread.startsWith("sync-sched-"), passes.syncThread);
            assertTrue(passes.otherThread.startsWith("schedule-pool-"), passes.otherThread);
        }
    }

    @Test
    void closingTheContextLetsAnInFlightPassFinishAndTriggersNoMore() throws Exception {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(FrameworkPool.class, SyncSchedulingConfig.class, SlowPass.class);
        context.refresh();
        SlowPass pass = context.getBean(SlowPass.class);
        assertTrue(pass.started.await(5, TimeUnit.SECONDS));

        context.close();

        assertTrue(pass.finished, "the in-flight pass must run to completion");
        assertFalse(pass.interrupted, "the in-flight pass must not be interrupted");
        int runs = pass.runs.get();
        Thread.sleep(150);
        assertEquals(runs, pass.runs.get(), "no pass may be triggered after the context closed");
    }

    /** Guards the routing: a new pass that forgets {@code scheduler = SCHEDULER} lands back on the shared pool. */
    @Test
    void everyScheduledPassOfTheModuleIsRoutedToTheModuleScheduler() throws Exception {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        CachingMetadataReaderFactory readers = new CachingMetadataReaderFactory(resolver);
        List<String> passes = new ArrayList<>();
        for (Resource resource : resolver.getResources("classpath*:org/dromara/sync/**/*.class")) {
            AnnotationMetadata type = readers.getMetadataReader(resource).getAnnotationMetadata();
            if (type.getClassName().endsWith("Test") || type.getClassName().contains("Test$")) continue;
            for (MethodMetadata method : type.getAnnotatedMethods(Scheduled.class.getName())) {
                String pass = type.getClassName() + "#" + method.getMethodName();
                assertEquals(SyncSchedulingConfig.SCHEDULER,
                    method.getAnnotationAttributes(Scheduled.class.getName()).get("scheduler"), pass);
                passes.add(pass);
            }
        }
        assertFalse(passes.isEmpty());
        // The default pool size promises one thread per pass; a new pass has to raise it.
        assertTrue(passes.size() <= SyncSchedulingConfig.DEFAULT_POOL_SIZE, passes.toString());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    static class FrameworkPool {

        @Bean(destroyMethod = "shutdownNow")
        ScheduledExecutorService scheduledExecutorService() {
            return new ScheduledThreadPoolExecutor(2, new CustomizableThreadFactory("schedule-pool-"));
        }
    }

    /** Same rule as {@code org.dromara.config.DevFastLazyInitConfig} in ruoyi-admin. */
    @Configuration(proxyBeanMethods = false)
    static class DevFastRules {

        @Bean
        static LazyInitializationExcludeFilter scheduledBeansStayEager() {
            return (beanName, beanDefinition, beanType) -> {
                for (Method method : beanType.getMethods()) {
                    if (method.isAnnotationPresent(Scheduled.class)) return true;
                }
                return false;
            };
        }
    }

    static class Passes {
        final CountDownLatch syncRan = new CountDownLatch(1);
        final CountDownLatch otherRan = new CountDownLatch(1);
        volatile String syncThread;
        volatile String otherThread;

        @Scheduled(fixedDelay = 50, scheduler = SyncSchedulingConfig.SCHEDULER)
        public void syncPass() {
            syncThread = Thread.currentThread().getName();
            syncRan.countDown();
        }

        @Scheduled(fixedDelay = 50)
        public void otherModulePass() {
            otherThread = Thread.currentThread().getName();
            otherRan.countDown();
        }
    }

    static class SlowPass {
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger();
        volatile boolean finished;
        volatile boolean interrupted;

        @Scheduled(fixedDelay = 10, scheduler = SyncSchedulingConfig.SCHEDULER)
        public void pass() {
            runs.incrementAndGet();
            started.countDown();
            try {
                Thread.sleep(300);
                finished = true;
            } catch (InterruptedException ex) {
                interrupted = true;
                Thread.currentThread().interrupt();
            }
        }
    }
}
