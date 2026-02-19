package com.doosan.erp.ocr.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(OcrProperties.class)
public class OcrPropertiesConfig {
}
