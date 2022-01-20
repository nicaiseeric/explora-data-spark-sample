package fr.iiil.bigdata.spark.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Contact implements Serializable {
    private String raison;
    private String word;
    private Long nb;
    private String address;
}
