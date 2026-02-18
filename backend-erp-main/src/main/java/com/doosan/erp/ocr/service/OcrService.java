package com.doosan.erp.ocr.service;

import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.exception.BusinessException;
import com.doosan.erp.ocr.client.AwsTextractClient;
import com.doosan.erp.ocr.dto.CellDto;
import com.doosan.erp.ocr.dto.DocumentAnalysisResponse;
import com.doosan.erp.ocr.dto.KeyValueDto;
import com.doosan.erp.ocr.dto.OcrResponse;
import com.doosan.erp.ocr.dto.TableDto;
import com.doosan.erp.ocr.dto.TextBlockDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.services.textract.model.AnalyzeDocumentResponse;
import software.amazon.awssdk.services.textract.model.Block;
import software.amazon.awssdk.services.textract.model.BlockType;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextResponse;
import software.amazon.awssdk.services.textract.model.EntityType;
import software.amazon.awssdk.services.textract.model.FeatureType;
import software.amazon.awssdk.services.textract.model.RelationshipType;
import software.amazon.awssdk.services.textract.model.TextractException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OCR 서비스
 *
 * Amazon Textract를 활용한 OCR(광학 문자 인식) 비즈니스 로직을 처리하는 서비스 클래스입니다.
 * 파일 검증, AWS API 호출, 응답 데이터 변환 등을 담당합니다.
 *
 * 주요 기능:
 * - 텍스트 추출: 이미지에서 텍스트 라인 추출 (DetectDocumentText API)
 * - 문서 분석: 테이블/폼 데이터 구조화 추출 (AnalyzeDocument API)
 *
 * 지원 파일 형식:
 * - PNG, JPG, JPEG, PDF
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OcrService {

    // AWS Textract API 호출을 위한 클라이언트
    private final AwsTextractClient awsTextractClient;

    // 지원하는 파일 형식 (MIME 타입)
    private static final Set<String> SUPPORTED_CONTENT_TYPES = Set.of(
            "image/png",
            "image/jpeg",
            "image/jpg",
            "application/pdf"
    );

    /**
     * 텍스트 추출
     *
     * 이미지 파일에서 텍스트 라인을 추출합니다.
     * Amazon Textract의 DetectDocumentText API를 사용합니다.
     *
     * @param file 텍스트를 추출할 이미지 파일
     * @return 추출된 텍스트와 블록별 상세 정보
     * @throws BusinessException 파일이 비어있거나 지원하지 않는 형식인 경우
     * @throws BusinessException Textract API 호출 실패 시
     */
    public OcrResponse extractText(MultipartFile file) {
        validateFile(file);

        try {
            byte[] fileBytes = file.getBytes();
            DetectDocumentTextResponse response = awsTextractClient.detectDocumentText(fileBytes);
            return buildOcrResponse(response);

        } catch (IOException e) {
            log.error("파일 읽기 실패: {}", e.getMessage());
            throw new BusinessException(ErrorCode.OCR_PROCESSING_FAILED, e);
        } catch (TextractException e) {
            log.error("Textract API 호출 실패: {}", e.getMessage());
            throw new BusinessException(ErrorCode.OCR_PROCESSING_FAILED, e);
        }
    }

    /**
     * 파일 유효성 검증
     *
     * 파일이 비어있는지, 지원하는 형식인지 검증합니다.
     *
     * @param file 검증할 파일
     * @throws BusinessException 파일이 비어있거나 지원하지 않는 형식인 경우
     */
    private void validateFile(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new BusinessException(ErrorCode.OCR_FILE_EMPTY);
        }

        String contentType = file.getContentType();
        if (contentType == null || !SUPPORTED_CONTENT_TYPES.contains(contentType.toLowerCase())) {
            throw new BusinessException(ErrorCode.OCR_INVALID_FILE,
                    "지원하는 파일 형식: PNG, JPG, JPEG, PDF");
        }
    }

    /**
     * Textract DetectDocumentText 응답을 OcrResponse로 변환
     *
     * LINE 타입 블록만 추출하여 응답 DTO를 생성합니다.
     *
     * @param response Textract API 응답
     * @return 변환된 OCR 응답 DTO
     */
    private OcrResponse buildOcrResponse(DetectDocumentTextResponse response) {
        List<Block> blocks = response.blocks();

        StringBuilder fullText = new StringBuilder();
        List<TextBlockDto> textBlocks = blocks.stream()
                .filter(block -> block.blockType() == BlockType.LINE)
                .map(block -> {
                    fullText.append(block.text()).append("\n");
                    return TextBlockDto.builder()
                            .text(block.text())
                            .confidence(block.confidence())
                            .blockType(block.blockTypeAsString())
                            .build();
                })
                .toList();

        float averageConfidence = (float) textBlocks.stream()
                .mapToDouble(TextBlockDto::getConfidence)
                .average()
                .orElse(0.0);

        return OcrResponse.builder()
                .extractedText(fullText.toString().trim())
                .blocks(textBlocks)
                .averageConfidence(averageConfidence)
                .build();
    }

    /**
     * 문서 분석 (테이블/폼 추출)
     *
     * 이미지 파일에서 테이블 데이터와 키-값 쌍(폼 필드)을 구조화된 형태로 추출합니다.
     * Amazon Textract의 AnalyzeDocument API를 사용하여 TABLES, FORMS 피처를 분석합니다.
     *
     * @param file 분석할 이미지 파일
     * @return 테이블, 키-값 쌍, 폼 필드가 포함된 분석 결과
     * @throws BusinessException 파일이 비어있거나 지원하지 않는 형식인 경우
     * @throws BusinessException Textract API 호출 실패 시
     */
    public DocumentAnalysisResponse analyzeDocument(MultipartFile file) {
        validateFile(file);

        try {
            byte[] fileBytes = file.getBytes();
            List<FeatureType> features = List.of(FeatureType.TABLES, FeatureType.FORMS);
            AnalyzeDocumentResponse response = awsTextractClient.analyzeDocument(fileBytes, features);
            return buildDocumentAnalysisResponse(response);

        } catch (IOException e) {
            log.error("파일 읽기 실패: {}", e.getMessage());
            throw new BusinessException(ErrorCode.OCR_PROCESSING_FAILED, e);
        } catch (TextractException e) {
            log.error("Textract API 호출 실패: {}", e.getMessage());
            throw new BusinessException(ErrorCode.OCR_PROCESSING_FAILED, e);
        }
    }

    /**
     * Textract AnalyzeDocument 응답을 DocumentAnalysisResponse로 변환
     *
     * LINE, TABLE, KEY_VALUE_SET 블록을 각각 처리하여 응답 DTO를 생성합니다.
     *
     * @param response Textract API 응답
     * @return 변환된 문서 분석 응답 DTO
     */
    private DocumentAnalysisResponse buildDocumentAnalysisResponse(AnalyzeDocumentResponse response) {
        List<Block> blocks = response.blocks();
        Map<String, Block> blockMap = blocks.stream()
                .collect(Collectors.toMap(Block::id, block -> block));

        // LINE 블록 추출
        StringBuilder fullText = new StringBuilder();
        List<TextBlockDto> lines = blocks.stream()
                .filter(block -> block.blockType() == BlockType.LINE)
                .map(block -> {
                    fullText.append(block.text()).append("\n");
                    return TextBlockDto.builder()
                            .text(block.text())
                            .confidence(block.confidence())
                            .blockType(block.blockTypeAsString())
                            .build();
                })
                .toList();

        // TABLE 추출
        List<TableDto> tables = extractTables(blocks, blockMap);

        // KEY_VALUE_SET (FORMS) 추출
        List<KeyValueDto> keyValuePairs = extractKeyValuePairs(blocks, blockMap);

        // formFields Map으로도 제공 (편의성)
        Map<String, String> formFields = keyValuePairs.stream()
                .collect(Collectors.toMap(
                        KeyValueDto::getKey,
                        KeyValueDto::getValue,
                        (v1, v2) -> v1,
                        LinkedHashMap::new
                ));

        float averageConfidence = (float) lines.stream()
                .mapToDouble(TextBlockDto::getConfidence)
                .average()
                .orElse(0.0);

        return DocumentAnalysisResponse.builder()
                .extractedText(fullText.toString().trim())
                .lines(lines)
                .tables(tables)
                .keyValuePairs(keyValuePairs)
                .formFields(formFields)
                .averageConfidence(averageConfidence)
                .build();
    }

    /**
     * TABLE 블록에서 테이블 데이터 추출
     *
     * TABLE 타입 블록을 찾아 각 셀의 텍스트와 위치 정보를 추출합니다.
     * 2차원 배열 형태의 rows와 헤더-값 매핑도 함께 생성합니다.
     *
     * @param blocks   전체 블록 목록
     * @param blockMap 블록 ID를 키로 하는 블록 맵
     * @return 추출된 테이블 목록
     */
    private List<TableDto> extractTables(List<Block> blocks, Map<String, Block> blockMap) {
        List<TableDto> tables = new ArrayList<>();
        int tableIndex = 0;

        List<Block> tableBlocks = blocks.stream()
                .filter(block -> block.blockType() == BlockType.TABLE)
                .toList();

        for (Block tableBlock : tableBlocks) {
            List<CellDto> cells = new ArrayList<>();
            int maxRow = 0;
            int maxCol = 0;

            List<String> cellIds = getChildIds(tableBlock, RelationshipType.CHILD);

            for (String cellId : cellIds) {
                Block cellBlock = blockMap.get(cellId);
                if (cellBlock != null && cellBlock.blockType() == BlockType.CELL) {
                    int rowIndex = cellBlock.rowIndex();
                    int colIndex = cellBlock.columnIndex();
                    maxRow = Math.max(maxRow, rowIndex);
                    maxCol = Math.max(maxCol, colIndex);

                    String cellText = getCellText(cellBlock, blockMap);
                    boolean isHeader = cellBlock.entityTypes() != null &&
                            cellBlock.entityTypes().contains(EntityType.COLUMN_HEADER);

                    cells.add(CellDto.builder()
                            .rowIndex(rowIndex)
                            .columnIndex(colIndex)
                            .text(cellText)
                            .confidence(cellBlock.confidence())
                            .isHeader(isHeader)
                            .build());
                }
            }

            // 2차원 배열 형태로 rows 구성
            List<List<String>> rows = new ArrayList<>();
            for (int r = 1; r <= maxRow; r++) {
                List<String> row = new ArrayList<>();
                for (int c = 1; c <= maxCol; c++) {
                    final int rowIdx = r;
                    final int colIdx = c;
                    String cellText = cells.stream()
                            .filter(cell -> cell.getRowIndex() == rowIdx && cell.getColumnIndex() == colIdx)
                            .map(CellDto::getText)
                            .findFirst()
                            .orElse("");
                    row.add(cellText);
                }
                rows.add(row);
            }

            // 헤더-값 매핑 (첫 번째 행이 헤더인 경우)
            Map<String, String> headerToFirstRowMap = new LinkedHashMap<>();
            if (rows.size() >= 2) {
                List<String> headers = rows.get(0);
                List<String> firstDataRow = rows.get(1);
                for (int i = 0; i < headers.size() && i < firstDataRow.size(); i++) {
                    String header = headers.get(i);
                    if (header != null && !header.isBlank()) {
                        headerToFirstRowMap.put(header.trim(), firstDataRow.get(i));
                    }
                }
            }

            tables.add(TableDto.builder()
                    .tableIndex(tableIndex++)
                    .rowCount(maxRow)
                    .columnCount(maxCol)
                    .cells(cells)
                    .rows(rows)
                    .headerToFirstRowMap(headerToFirstRowMap)
                    .build());
        }

        return tables;
    }

    /**
     * KEY_VALUE_SET 블록에서 키-값 쌍 추출
     *
     * 폼 필드(레이블-값 쌍)를 추출합니다.
     * 예: "Order No:" → "528003-1322"
     *
     * @param blocks   전체 블록 목록
     * @param blockMap 블록 ID를 키로 하는 블록 맵
     * @return 추출된 키-값 쌍 목록
     */
    private List<KeyValueDto> extractKeyValuePairs(List<Block> blocks, Map<String, Block> blockMap) {
        List<KeyValueDto> keyValuePairs = new ArrayList<>();

        List<Block> keyBlocks = blocks.stream()
                .filter(block -> block.blockType() == BlockType.KEY_VALUE_SET)
                .filter(block -> block.entityTypes() != null && block.entityTypes().contains(EntityType.KEY))
                .toList();

        for (Block keyBlock : keyBlocks) {
            String keyText = getTextFromBlock(keyBlock, blockMap);
            Float keyConfidence = keyBlock.confidence();

            String valueText = "";
            Float valueConfidence = null;

            List<String> valueIds = getChildIds(keyBlock, RelationshipType.VALUE);
            if (!valueIds.isEmpty()) {
                Block valueBlock = blockMap.get(valueIds.get(0));
                if (valueBlock != null) {
                    valueText = getTextFromBlock(valueBlock, blockMap);
                    valueConfidence = valueBlock.confidence();
                }
            }

            if (keyText != null && !keyText.isBlank()) {
                keyValuePairs.add(KeyValueDto.builder()
                        .key(keyText.trim())
                        .value(valueText != null ? valueText.trim() : "")
                        .keyConfidence(keyConfidence)
                        .valueConfidence(valueConfidence)
                        .build());
            }
        }

        return keyValuePairs;
    }

    /**
     * 셀 블록에서 텍스트 추출
     *
     * @param cellBlock 셀 블록
     * @param blockMap  블록 맵
     * @return 셀의 텍스트
     */
    private String getCellText(Block cellBlock, Map<String, Block> blockMap) {
        return getTextFromBlock(cellBlock, blockMap);
    }

    /**
     * 블록에서 텍스트 추출
     *
     * 블록의 자식 WORD 블록들을 조합하여 텍스트를 생성합니다.
     *
     * @param block    텍스트를 추출할 블록
     * @param blockMap 블록 맵
     * @return 추출된 텍스트
     */
    private String getTextFromBlock(Block block, Map<String, Block> blockMap) {
        List<String> childIds = getChildIds(block, RelationshipType.CHILD);
        if (childIds.isEmpty()) {
            return block.text() != null ? block.text() : "";
        }

        StringBuilder text = new StringBuilder();
        for (String childId : childIds) {
            Block childBlock = blockMap.get(childId);
            if (childBlock != null && childBlock.blockType() == BlockType.WORD) {
                if (text.length() > 0) {
                    text.append(" ");
                }
                text.append(childBlock.text());
            }
        }
        return text.toString();
    }

    /**
     * 블록의 자식 ID 목록 조회
     *
     * 특정 관계 타입(CHILD, VALUE 등)에 해당하는 자식 블록 ID 목록을 반환합니다.
     *
     * @param block 부모 블록
     * @param type  관계 타입
     * @return 자식 블록 ID 목록
     */
    private List<String> getChildIds(Block block, RelationshipType type) {
        if (block.relationships() == null) {
            return List.of();
        }

        return block.relationships().stream()
                .filter(rel -> rel.type() == type)
                .flatMap(rel -> rel.ids().stream())
                .toList();
    }
}
