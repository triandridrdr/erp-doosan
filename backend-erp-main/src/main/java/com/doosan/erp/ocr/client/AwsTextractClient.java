package com.doosan.erp.ocr.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.AnalyzeDocumentRequest;
import software.amazon.awssdk.services.textract.model.AnalyzeDocumentResponse;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextRequest;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextResponse;
import software.amazon.awssdk.services.textract.model.Document;
import software.amazon.awssdk.services.textract.model.FeatureType;

import java.util.List;

/**
 * AWS Textract 클라이언트
 *
 * Amazon Textract API를 호출하는 래퍼 클래스입니다.
 * AWS SDK의 TextractClient를 사용하여 OCR 기능을 제공합니다.
 *
 * 제공 API:
 * - detectDocumentText: 이미지에서 텍스트 감지 (단순 텍스트 추출)
 * - analyzeDocument: 문서 분석 (테이블, 폼 필드 추출)
 *
 * 의존성:
 * - TextractClient: AwsConfig에서 Bean으로 등록
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AwsTextractClient {

    // AWS SDK Textract 클라이언트 (AwsConfig에서 Bean 주입)
    private final TextractClient textractClient;

    /**
     * 문서 텍스트 감지 (DetectDocumentText API)
     *
     * 이미지에서 텍스트를 감지하여 LINE, WORD 블록으로 반환합니다.
     * 단순 텍스트 추출에 적합하며, 테이블/폼 분석에는 analyzeDocument를 사용하세요.
     *
     * @param imageBytes 이미지 바이트 배열
     * @return Textract API 응답 (블록 목록 포함)
     */
    public DetectDocumentTextResponse detectDocumentText(byte[] imageBytes) {
        log.debug("Textract API 호출 시작");

        Document document = Document.builder()
                .bytes(SdkBytes.fromByteArray(imageBytes))
                .build();

        DetectDocumentTextRequest request = DetectDocumentTextRequest.builder()
                .document(document)
                .build();

        DetectDocumentTextResponse response = textractClient.detectDocumentText(request);

        log.debug("Textract API 호출 완료 - 블록 수: {}", response.blocks().size());

        return response;
    }

    /**
     * 문서 분석 (AnalyzeDocument API)
     *
     * 이미지에서 테이블, 폼 필드 등 구조화된 데이터를 추출합니다.
     * 피처 타입에 따라 TABLE, KEY_VALUE_SET 등의 블록을 반환합니다.
     *
     * @param imageBytes   이미지 바이트 배열
     * @param featureTypes 분석할 피처 타입 목록 (TABLES, FORMS 등)
     * @return Textract API 응답 (테이블, 폼 필드 블록 포함)
     */
    public AnalyzeDocumentResponse analyzeDocument(byte[] imageBytes, List<FeatureType> featureTypes) {
        log.debug("Textract AnalyzeDocument API 호출 시작 - 피처: {}", featureTypes);

        Document document = Document.builder()
                .bytes(SdkBytes.fromByteArray(imageBytes))
                .build();

        AnalyzeDocumentRequest request = AnalyzeDocumentRequest.builder()
                .document(document)
                .featureTypes(featureTypes)
                .build();

        AnalyzeDocumentResponse response = textractClient.analyzeDocument(request);

        log.debug("Textract AnalyzeDocument API 호출 완료 - 블록 수: {}", response.blocks().size());

        return response;
    }
}
