package com.doosan.erp.style.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "style")
@Getter
@Setter
@NoArgsConstructor
public class Style {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "style_id")
    private Long id;

    @Column(name = "product_id")
    private String productId;

    @Column(name = "style_code", length = 50)
    private String styleCode;

    @Column(name = "style_name", length = 200)
    private String styleName;

    @Column(name = "season", length = 50)
    private String season;

    @Column(name = "description", length = 1000)
    private String description;
}
