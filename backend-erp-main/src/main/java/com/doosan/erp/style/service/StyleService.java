package com.doosan.erp.style.service;

import com.doosan.erp.common.constant.ErrorCode;
import com.doosan.erp.common.dto.PageResponse;
import com.doosan.erp.common.exception.ResourceNotFoundException;
import com.doosan.erp.style.dto.StyleRequest;
import com.doosan.erp.style.dto.StyleResponse;
import com.doosan.erp.style.entity.Style;
import com.doosan.erp.style.repository.StyleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class StyleService {

    private final StyleRepository styleRepository;

    @Transactional
    public StyleResponse create(StyleRequest request) {
        log.info("Creating style: {}", request.getStyleCode());

        Style style = new Style();
        style.setProductId(request.getProductId());
        style.setStyleCode(request.getStyleCode());
        style.setStyleName(request.getStyleName());
        style.setSeason(request.getSeason());
        style.setDescription(request.getDescription());
        style.setDefaultBomMasterId(request.getDefaultBomMasterId());

        Style saved = styleRepository.save(style);
        return StyleResponse.from(saved);
    }

    public StyleResponse getOne(Long id) {
        Style style = styleRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.RESOURCE_NOT_FOUND, "Style not found"));
        return StyleResponse.from(style);
    }

    public PageResponse<StyleResponse> search(String search, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        Page<Style> stylePage = styleRepository.search(search, pageable);

        List<StyleResponse> content = stylePage.getContent().stream()
                .map(StyleResponse::from)
                .collect(Collectors.toList());

        return PageResponse.of(content, page, size, stylePage.getTotalElements());
    }

    @Transactional
    public StyleResponse update(Long id, StyleRequest request) {
        Style style = styleRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException(ErrorCode.RESOURCE_NOT_FOUND, "Style not found"));

        style.setProductId(request.getProductId());
        style.setStyleCode(request.getStyleCode());
        style.setStyleName(request.getStyleName());
        style.setSeason(request.getSeason());
        style.setDescription(request.getDescription());
        style.setDefaultBomMasterId(request.getDefaultBomMasterId());

        Style saved = styleRepository.save(style);
        return StyleResponse.from(saved);
    }

    @Transactional
    public void delete(Long id) {
        if (!styleRepository.existsById(id)) {
            throw new ResourceNotFoundException(ErrorCode.RESOURCE_NOT_FOUND, "Style not found");
        }
        styleRepository.deleteById(id);
    }
}
