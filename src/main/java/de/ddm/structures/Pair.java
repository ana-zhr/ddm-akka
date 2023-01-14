package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Pair {

    private int tableDependentId;
    private int tableIndId;

    private String dependent;

    private String referencedId;


}
