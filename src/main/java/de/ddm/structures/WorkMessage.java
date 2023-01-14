package de.ddm.structures;

import de.ddm.serialization.AkkaSerializable;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
public class WorkMessage implements AkkaSerializable {
    List<String[]> content;
    private int id;
    private String[] head;


    public WorkMessage(List<String[]> content, int id, String[] head) {
        this.content = content;
        this.id = id;
        this.head = head;
    }


    public String col(int index) {
        return head[index];
    }

    public String elem(int row, int col) {
        return content.get(row)[col];
    }

    public int columns() {
        return head.length;
    }

    public int rows() {
        return content.size();
    }
}
