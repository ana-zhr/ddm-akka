package de.ddm.structures;

import java.util.*;
import java.util.stream.Stream;

public class LocalDataStorage {
    private Map<String, List<String>> headerList = new HashMap<>();
    private Map<String, List<Column>> contentList = new HashMap<>();

    public LocalDataStorage() {}

    public void addTable(String tableName, List<String> header) {
        this.headerList.put(tableName, header);

        List<Column> content = new ArrayList<>();
        for (String colName : header)
            content.add(new Column());
        this.contentList.put(tableName, content);
    }

    public List<String> getTableNames() {
        List<String> tableNames = new ArrayList<>();
        for (String key : headerList.keySet()) {
            tableNames.add(key);
        }
        return tableNames;
    }

    public List<String> getHeader(String tableName) {
        return this.headerList.get(tableName);
    }

    public void addRow(String tableName, List<String> values) {
        List<Column> content = this.contentList.get(tableName);
        assert values.size() == content.size()
            : "row length " + values.size() + " does not match header length " + content.size();

        for (int i = 0; i < values.size(); ++i) {
            content.get(i).add(values.get(i));
        }
    }

    public void addRows(String tableName, Stream<List<String>> rows) {
        List<Column> content = this.contentList.get(tableName);
        rows.forEach(values -> {
            assert values.size() == content.size()
            : "row length " + values.size() + " does not match header length " + content.size();

            for (int i = 0; i < values.size(); ++i) {
                content.get(i).add(values.get(i));
            }
        });
    }

    public Column getColumn(String tableName, String columnName) {
        int index = headerList.get(tableName).indexOf(columnName);
        return this.contentList.get(tableName).get(index);
    }


}
