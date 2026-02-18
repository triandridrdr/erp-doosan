package com.doosan.erp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.textract.TextractClient;

@Configuration
public class AwsTextractConfig {

    @Value("${aws.credentials.access-key:}")
    private String accessKey;

    @Value("${aws.credentials.secret-key:}")
    private String secretKey;

    @Value("${aws.textract-region:ap-southeast-1}")
    private String region;

    @Bean
    public TextractClient textractClient() {
        return TextractClient.builder()
                .region(Region.of(region))
                .credentialsProvider(awsCredentialsProvider())
                .build();
    }

    private AwsCredentialsProvider awsCredentialsProvider() {
        if (StringUtils.hasText(accessKey) && StringUtils.hasText(secretKey)) {
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)
            );
        }
        return DefaultCredentialsProvider.create();
    }
}
