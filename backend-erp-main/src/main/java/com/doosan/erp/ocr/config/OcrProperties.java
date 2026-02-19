package com.doosan.erp.ocr.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "ocr")
public class OcrProperties {

    private Engine engine = Engine.aws;

    private Python python = new Python();

    public enum Engine {
        aws,
        python
    }

    @Getter
    @Setter
    public static class Python {
        private String baseUrl = "http://localhost:8001";
        private String engine = "tesseract";
        private boolean preprocess = true;
    }
}
