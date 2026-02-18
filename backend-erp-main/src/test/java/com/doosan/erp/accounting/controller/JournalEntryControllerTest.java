package com.doosan.erp.accounting.controller;

import com.doosan.erp.accounting.dto.JournalEntryRequest;
import com.doosan.erp.accounting.dto.JournalEntryResponse;
import com.doosan.erp.accounting.service.JournalEntryService;
import com.doosan.erp.common.dto.PageResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 회계전표 Controller 테스트
 */
@WebMvcTest(controllers = JournalEntryController.class, excludeAutoConfiguration = {
                org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
                org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
                org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class
})
class JournalEntryControllerTest {

        @Autowired
        private MockMvc mockMvc;

        @Autowired
        private ObjectMapper objectMapper;

        @MockitoBean
        private JournalEntryService journalEntryService;

        @Test
        @DisplayName("회계전표 생성 성공")
        void createJournalEntry_Success() throws Exception {
                // Given
                JournalEntryRequest.JournalEntryLineRequest lineRequest1 = new JournalEntryRequest.JournalEntryLineRequest(
                                1, "1000", "현금", BigDecimal.valueOf(100000), BigDecimal.ZERO, "비고1");
                JournalEntryRequest.JournalEntryLineRequest lineRequest2 = new JournalEntryRequest.JournalEntryLineRequest(
                                2, "4000", "매출", BigDecimal.ZERO, BigDecimal.valueOf(100000), "비고2");

                JournalEntryRequest request = new JournalEntryRequest(
                                LocalDate.now(),
                                "급여 지급 처리",
                                List.of(lineRequest1, lineRequest2));

                JournalEntryResponse.JournalEntryLineResponse lineResponse1 = new JournalEntryResponse.JournalEntryLineResponse(
                                1L, 1, "1000", "현금", BigDecimal.valueOf(100000), BigDecimal.ZERO, "비고1");
                JournalEntryResponse.JournalEntryLineResponse lineResponse2 = new JournalEntryResponse.JournalEntryLineResponse(
                                2L, 2, "4000", "매출", BigDecimal.ZERO, BigDecimal.valueOf(100000), "비고2");

                JournalEntryResponse response = new JournalEntryResponse(
                                1L, "JE-2025-0001", LocalDate.now(), "DRAFT",
                                "급여 지급 처리",
                                BigDecimal.valueOf(100000),
                                BigDecimal.valueOf(100000),
                                List.of(lineResponse1, lineResponse2),
                                null, "admin");

                when(journalEntryService.createJournalEntry(any())).thenReturn(response);

                // When & Then
                mockMvc.perform(post("/api/v1/accounting/journal-entries")
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(objectMapper.writeValueAsString(request)))
                                .andDo(print())
                                .andExpect(status().isCreated())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.entryNumber").value("JE-2025-0001"))
                                .andExpect(jsonPath("$.data.status").value("DRAFT"));
        }

        @Test
        @DisplayName("회계전표 조회 성공")
        void getJournalEntry_Success() throws Exception {
                // Given
                Long entryId = 1L;

                JournalEntryResponse.JournalEntryLineResponse lineResponse = new JournalEntryResponse.JournalEntryLineResponse(
                                1L, 1, "1000", "현금", BigDecimal.valueOf(100000), BigDecimal.ZERO, "비고");

                JournalEntryResponse response = new JournalEntryResponse(
                                entryId, "JE-2025-0001", LocalDate.now(), "DRAFT",
                                "급여 지급 처리",
                                BigDecimal.valueOf(100000),
                                BigDecimal.valueOf(100000),
                                List.of(lineResponse),
                                null, "admin");

                when(journalEntryService.getJournalEntry(entryId)).thenReturn(response);

                // When & Then
                mockMvc.perform(get("/api/v1/accounting/journal-entries/{id}", entryId))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.id").value(entryId))
                                .andExpect(jsonPath("$.data.entryNumber").value("JE-2025-0001"));
        }

        @Test
        @DisplayName("회계전표 전기 성공")
        void postJournalEntry_Success() throws Exception {
                // Given
                Long entryId = 1L;

                JournalEntryResponse.JournalEntryLineResponse lineResponse = new JournalEntryResponse.JournalEntryLineResponse(
                                1L, 1, "1000", "현금", BigDecimal.valueOf(100000), BigDecimal.ZERO, "비고");

                JournalEntryResponse response = new JournalEntryResponse(
                                entryId, "JE-2025-0001", LocalDate.now(), "POSTED",
                                "급여 지급 처리",
                                BigDecimal.valueOf(100000),
                                BigDecimal.valueOf(100000),
                                List.of(lineResponse),
                                null, "admin");

                when(journalEntryService.postJournalEntry(eq(entryId))).thenReturn(response);

                // When & Then
                mockMvc.perform(post("/api/v1/accounting/journal-entries/{id}/post", entryId))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.status").value("POSTED"))
                                .andExpect(jsonPath("$.message").value("회계전표가 전기되었습니다"));
        }

        @Test
        @DisplayName("회계전표 목록 조회 성공")
        void getJournalEntries_Success() throws Exception {
                // Given
                JournalEntryResponse response = new JournalEntryResponse(
                                1L, "JE-2025-0001", LocalDate.now(), "DRAFT",
                                "급여 지급 처리",
                                BigDecimal.valueOf(100000),
                                BigDecimal.valueOf(100000),
                                List.of(),
                                null, "admin");

                PageResponse<JournalEntryResponse> pageResponse = new PageResponse<>(
                                List.of(response), 0, 10, 1, 1, true, true);

                when(journalEntryService.getJournalEntries(0, 10)).thenReturn(pageResponse);

                // When & Then
                mockMvc.perform(get("/api/v1/accounting/journal-entries")
                                .param("page", "0")
                                .param("size", "10"))
                                .andDo(print())
                                .andExpect(status().isOk())
                                .andExpect(jsonPath("$.success").value(true))
                                .andExpect(jsonPath("$.data.content[0].entryNumber").value("JE-2025-0001"));
        }
}
