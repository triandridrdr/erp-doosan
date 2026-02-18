package com.doosan.erp.common.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 페이징 응답 DTO
 *
 * 목록 조회 시 페이징 정보와 함께 데이터를 반환합니다.
 *
 * 포함 정보:
 * - content: 현재 페이지의 데이터 목록
 * - page: 현재 페이지 번호 (0부터 시작)
 * - size: 페이지당 항목 수
 * - totalElements: 전체 항목 수
 * - totalPages: 전체 페이지 수
 * - first/last: 첫/마지막 페이지 여부
 *
 * @param <T> 목록 항목의 타입
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class PageResponse<T> {

    private List<T> content;      // 현재 페이지 데이터
    private int page;             // 현재 페이지 번호
    private int size;             // 페이지 크기
    private long totalElements;   // 전체 항목 수
    private int totalPages;       // 전체 페이지 수
    private boolean first;        // 첫 페이지 여부
    private boolean last;         // 마지막 페이지 여부

    /**
     * PageResponse 생성 팩토리 메서드
     *
     * @param content 현재 페이지 데이터
     * @param page 현재 페이지 번호
     * @param size 페이지 크기
     * @param totalElements 전체 항목 수
     * @return PageResponse 객체
     */
    public static <T> PageResponse<T> of(List<T> content, int page, int size, long totalElements) {
        int totalPages = (int) Math.ceil((double) totalElements / size);
        boolean first = page == 0;
        boolean last = page >= totalPages - 1;

        return new PageResponse<>(content, page, size, totalElements, totalPages, first, last);
    }
}
