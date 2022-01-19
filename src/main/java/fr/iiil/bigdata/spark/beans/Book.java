package fr.iiil.bigdata.spark.beans;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Book implements Serializable {
    private String title;
    private Float price;
    private Integer nbpages;
}
