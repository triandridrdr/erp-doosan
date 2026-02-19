package com.doosan.erp.ocr.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.doosan.erp.ocr.dto.python.PythonOcrExtractResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
@RequiredArgsConstructor
public class PythonOcrClient {

    private final ObjectMapper objectMapper;

    public PythonOcrExtractResponse extract(String baseUrl, MultipartFile file, String engine, boolean preprocess) throws IOException {
        String originalFilename = file.getOriginalFilename() != null ? file.getOriginalFilename() : "uploaded";
        String safeFilename = originalFilename.replace("\"", "");

        ByteArrayResource resource = new ByteArrayResource(file.getBytes()) {
            @Override
            public String getFilename() {
                return safeFilename;
            }
        };

        String fileContentType = file.getContentType() != null ? file.getContentType() : MediaType.APPLICATION_OCTET_STREAM_VALUE;

        byte[] bodyBytes = resource.getByteArray();

        String encodedEngine = URLEncoder.encode(engine, StandardCharsets.UTF_8);
        String url = baseUrl + "/ocr/extract?engine=" + encodedEngine + "&preprocess=" + preprocess;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("Accept", MediaType.APPLICATION_JSON_VALUE)
                .header("Content-Type", fileContentType)
                .header("X-Filename", safeFilename)
                .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
                .build();

        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(20))
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();

            HttpResponse<String> httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (httpResponse.statusCode() >= 400) {
                throw new IOException("Python OCR service returned " + httpResponse.statusCode() + ": " + httpResponse.body());
            }

            return objectMapper.readValue(httpResponse.body(), PythonOcrExtractResponse.class);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Python OCR call interrupted", e);
        }
    }
}
