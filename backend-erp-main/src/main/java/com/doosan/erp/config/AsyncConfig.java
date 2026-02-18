package com.doosan.erp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * 비동기 처리 설정 클래스
 *
 * Async 어노테이션을 사용한 비동기 메서드 실행을 위한 설정입니다.
 * 도메인 이벤트의 비동기 처리에 사용됩니다.
 *
 * 예시: 수주 생성 시 재고 예약 이벤트를 비동기로 처리
 */
@Configuration
@EnableAsync  // Async 어노테이션 활성화
public class AsyncConfig {

    /**
     * 비동기 작업 실행을 위한 스레드 풀 설정
     *
     * 스레드 풀 구성:
     * - corePoolSize: 기본 스레드 수 (5개)
     * - maxPoolSize: 최대 스레드 수 (10개)
     * - queueCapacity: 대기 큐 크기 (100개)
     * - threadNamePrefix: 스레드 이름 접두사
     *
     * Async 어노테이션이 붙은 메서드는 이 스레드 풀에서 실행됩니다.
     *
     * @return 설정된 TaskExecutor
     */
    @Bean(name = "taskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);       // 기본 스레드 5개 유지
        executor.setMaxPoolSize(10);       // 최대 10개까지 확장
        executor.setQueueCapacity(100);    // 대기 가능한 작업 100개
        executor.setThreadNamePrefix("event-async-");  // 로그에서 식별용
        executor.initialize();
        return executor;
    }
}
