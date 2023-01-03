package de.ddm.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.EqualsAndHashCode;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class InclusionDependency {
    private String dependentTable;
    private String referencedTable;


    private String dependentColumn;


    private String referencedColumn;

	@Override
	public String toString() {
		return this.dependentTable + " → " + this.referencedTable + ": " +
				this.dependentColumn + " ⊆ " + this.referencedColumn;
	}
}
