//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.debezium.relational;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.schema.DataCollectionId;

@Immutable
public final class TableId implements DataCollectionId, Comparable<TableId> {
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String id;

    public static TableId parse(String str) {
        return parse(str, true);
    }

    public static TableId parse(String str, boolean useCatalogBeforeSchema) {
        String[] parts = (String[])TableIdParser.parse(str).stream().toArray((x$0) -> {
            return new String[x$0];
        });
        return parse(parts, parts.length, useCatalogBeforeSchema);
    }

    protected static TableId parse(String[] parts, int numParts, boolean useCatalogBeforeSchema) {
        if (numParts == 0) {
            return null;
        } else if (numParts == 1) {
            return new TableId((String)null, (String)null, parts[0]);
        } else if (numParts == 2) {
            return useCatalogBeforeSchema ? new TableId(parts[0], (String)null, parts[1]) : new TableId((String)null, parts[0], parts[1]);
        } else {
            return new TableId(parts[0], parts[1], parts[2]);
        }
    }

    public TableId(String catalogName, String schemaName, String tableName, TableIdToStringMapper tableIdMapper) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;

        assert this.tableName != null;

        this.id = tableIdMapper == null ? tableId(this.catalogName, this.schemaName, this.tableName) : tableIdMapper.toString(this);
    }

    public TableId(String catalogName, String schemaName, String tableName) {
        this(catalogName, schemaName, tableName, (TableIdToStringMapper)null);
    }

    public String catalog() {
        return this.catalogName;
    }

    public String schema() {
        return this.schemaName;
    }

    public String table() {
        return this.tableName;
    }

    public String identifier() {
        return this.id;
    }

    public int compareTo(TableId that) {
        return this == that ? 0 : this.id.compareTo(that.id);
    }

    public int compareToIgnoreCase(TableId that) {
        return this == that ? 0 : this.id.compareToIgnoreCase(that.id);
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof TableId) {
            return this.compareTo((TableId)obj) == 0;
        } else {
            return false;
        }
    }

    public String toString() {
        return this.identifier();
    }

    public String toDoubleQuotedString() {
        return this.toQuotedString('"');
    }

    public TableId toDoubleQuoted() {
        return this.toQuoted('"');
    }

    public TableId toQuoted(char quotingChar) {
        String catalogName = null;
        if (this.catalogName != null && !this.catalogName.isEmpty()) {
            catalogName = quote(this.catalogName, quotingChar);
        }

        String schemaName = null;
        if (this.schemaName != null && !this.schemaName.isEmpty()) {
            schemaName = quote(this.schemaName, quotingChar);
        }

        return new TableId(catalogName, schemaName, quote(this.tableName, quotingChar));
    }

    public String toQuotedString(char quotingChar) {
        StringBuilder quoted = new StringBuilder();
        if (this.catalogName != null && !this.catalogName.isEmpty()) {
            quoted.append(quote(this.catalogName, quotingChar)).append(".");
        }

        if (this.schemaName != null && !this.schemaName.isEmpty()) {
            quoted.append(quote(this.schemaName, quotingChar)).append(".");
        }

        quoted.append(quote(this.tableName, quotingChar));
        return quoted.toString();
    }

    private static String tableId(String catalog, String schema, String table) {
        if (catalog != null && catalog.length() != 0) {
            return schema != null && schema.length() != 0 ? catalog + "." + schema + "." + table : catalog + "." + table;
        } else {
            return schema != null && schema.length() != 0 ? schema + "." + table : table;
        }
    }

    private static String quote(String identifierPart, char quotingChar) {
        if (identifierPart == null) {
            return null;
        } else if (identifierPart.isEmpty()) {
            return "" + quotingChar + quotingChar;
        } else {
            if (identifierPart.charAt(0) != quotingChar && identifierPart.charAt(identifierPart.length() - 1) != quotingChar) {
                identifierPart = identifierPart.replace(quotingChar + "", repeat(quotingChar));
                identifierPart = quotingChar + identifierPart + quotingChar;
            }

            return identifierPart;
        }
    }

    private static String repeat(char quotingChar) {
        return "" + quotingChar + quotingChar;
    }

    public TableId toLowercase() {
        return new TableId(this.catalogName, this.schemaName, this.tableName.toUpperCase());
    }
}
