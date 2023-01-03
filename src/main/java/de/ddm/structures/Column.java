package de.ddm.structures;

import lombok.NoArgsConstructor;

import java.util.*;
import java.util.stream.Stream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@NoArgsConstructor
public class Column {


    private List<String> valuesByPosition = new ArrayList<>();
    private Map<Integer, Integer> positionByValueHash = new HashMap<>();

    private List<Integer> data = new ArrayList<>();


    private int memorySize = 0;

    public void add(String value){
        int index = this.positionByValueHash.computeIfAbsent(value.hashCode(), _valueHash -> {
            valuesByPosition.add(value);
            this.memorySize += value.length() * 2;
            return valuesByPosition.size() - 1;
        });
        data.add(index);
        this.memorySize += 4;
    }

    public Stream<String> stream() {
        return data.stream()
            .map(index -> valuesByPosition.get(index));
    }

    public int size() {
        return this.data.size();
    }

    public int getMemorySize(){
        return this.memorySize;
    }

    public Set<String> getDistinctValues() {
        return new HashSet<>(this.valuesByPosition);
    }

    @Override
    public String toString(){
        return Arrays.toString(this.stream().toArray());
    }
}
