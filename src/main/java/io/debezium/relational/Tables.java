//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.debezium.relational;

import io.debezium.annotation.ThreadSafe;
import io.debezium.function.Predicates;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.util.Collect;
import io.debezium.util.FunctionalReadWriteLock;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

@ThreadSafe
public final class Tables {
    private final FunctionalReadWriteLock lock;
    private final Tables.TablesById tablesByTableId;
    private final Tables.TableIds changes;
    private final boolean tableIdCaseInsensitive;

    public Tables(boolean tableIdCaseInsensitive) {
        this.lock = FunctionalReadWriteLock.reentrant();
        this.tableIdCaseInsensitive = tableIdCaseInsensitive;
        this.tablesByTableId = new Tables.TablesById(tableIdCaseInsensitive);
        this.changes = new Tables.TableIds(tableIdCaseInsensitive);
    }

    public Tables() {
        this(false);
    }

    protected Tables(Tables other, boolean tableIdCaseInsensitive) {
        this(tableIdCaseInsensitive);
        this.tablesByTableId.putAll(other.tablesByTableId);
    }

    public void clear() {
        this.lock.write(() -> {
            this.tablesByTableId.clear();
            this.changes.clear();
        });
    }

    public Tables clone() {
        return new Tables(this, this.tableIdCaseInsensitive);
    }

    public int size() {
        FunctionalReadWriteLock var10000 = this.lock;
        Tables.TablesById var10001 = this.tablesByTableId;
        var10001.getClass();
        return (Integer)var10000.read(var10001::size);
    }

    public Set<TableId> drainChanges() {
        return (Set)this.lock.write(() -> {
            Set<TableId> result = this.changes.toSet();
            this.changes.clear();
            return result;
        });
    }

    public Table overwriteTable(TableId tableId, List<Column> columnDefs, List<String> primaryKeyColumnNames, String defaultCharsetName) {
        return (Table)this.lock.write(() -> {
            Table updated = Table.editor().tableId(tableId).addColumns(columnDefs).setPrimaryKeyNames(primaryKeyColumnNames).setDefaultCharsetName(defaultCharsetName).create();
            Table existing = this.tablesByTableId.get(tableId);
            if (existing == null || !existing.equals(updated)) {
                this.changes.add(tableId);
                this.tablesByTableId.put(tableId, updated);
            }

            return this.tablesByTableId.get(tableId);
        });
    }

    public Table overwriteTable(Table table) {
        return (Table)this.lock.write(() -> {
            TableImpl updated = new TableImpl(table);

            Table var3;
            try {
                var3 = this.tablesByTableId.put(updated.id(), updated);
            } finally {
                this.changes.add(updated.id());
            }

            return var3;
        });
    }

    public void removeTablesForDatabase(String schemaName) {
        this.removeTablesForDatabase(schemaName, (String)null);
    }

    public void removeTablesForDatabase(String catalogName, String schemaName) {
        this.lock.write(() -> {
            this.tablesByTableId.entrySet().removeIf((tableIdTableEntry) -> {
                TableId tableId = (TableId)tableIdTableEntry.getKey();
                boolean equalCatalog = Objects.equals(catalogName, tableId.catalog());
                boolean equalSchema = Objects.equals(schemaName, tableId.schema());
                return equalSchema && equalCatalog;
            });
        });
    }

    public Table renameTable(TableId existingTableId, TableId newTableId) {
        return (Table)this.lock.write(() -> {
            Table existing = this.forTable(existingTableId);
            if (existing == null) {
                return null;
            } else {
                this.tablesByTableId.remove(existing.id());
                TableImpl updated = new TableImpl(newTableId, existing.columns(), existing.primaryKeyColumnNames(), existing.defaultCharsetName());

                Table var5;
                try {
                    var5 = this.tablesByTableId.put(updated.id(), updated);
                } finally {
                    this.changes.add(existingTableId);
                    this.changes.add(updated.id());
                }

                return var5;
            }
        });
    }

    public Table updateTable(TableId tableId, Function<Table, Table> changer) {
        return (Table)this.lock.write(() -> {
            Table existing = this.tablesByTableId.get(tableId);
            Table updated = (Table)changer.apply(existing);
            if (updated != existing) {
                this.tablesByTableId.put(tableId, new TableImpl(tableId, updated.columns(), updated.primaryKeyColumnNames(), updated.defaultCharsetName()));
            }

            this.changes.add(tableId);
            return existing;
        });
    }

    public Table removeTable(TableId tableId) {
        return (Table)this.lock.write(() -> {
            this.changes.add(tableId);
            return this.tablesByTableId.remove(tableId);
        });
    }

    public Table forTable(TableId tableId) {
        return (Table)this.lock.read(() -> {
            return this.tablesByTableId.get(tableId);
        });
    }

    public Table forTable(String catalogName, String schemaName, String tableName) {
        return this.forTable(new TableId(catalogName, schemaName, tableName));
    }

    public Set<TableId> tableIds() {
        return (Set)this.lock.read(() -> {
            return Collect.unmodifiableSet(this.tablesByTableId.ids());
        });
    }

    public TableEditor editTable(TableId tableId) {
        Table table = this.forTable(tableId);
        return table == null ? null : table.edit();
    }

    public TableEditor editOrCreateTable(TableId tableId) {
        Table table = this.forTable(tableId);
        return table == null ? Table.editor().tableId(tableId) : table.edit();
    }

    public int hashCode() {
        return this.tablesByTableId.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof Tables) {
            Tables that = (Tables)obj;
            return this.tablesByTableId.equals(that.tablesByTableId);
        } else {
            return false;
        }
    }

    public Tables subset(Tables.TableFilter filter) {
        return filter == null ? this : (Tables)this.lock.read(() -> {
            Tables result = new Tables(this.tableIdCaseInsensitive);
            this.tablesByTableId.forEach((tableId, table) -> {
                if (filter.isIncluded(tableId)) {
                    result.overwriteTable(table);
                }

            });
            return result;
        });
    }

    public String toString() {
        return (String)this.lock.read(() -> {
            StringBuilder sb = new StringBuilder();
            sb.append("Tables {");
            if (!this.tablesByTableId.isEmpty()) {
                sb.append(System.lineSeparator());
                this.tablesByTableId.forEach((tableId, table) -> {
                    sb.append("  ").append(tableId).append(": {").append(System.lineSeparator());
                    if (table instanceof TableImpl) {
                        ((TableImpl)table).toString(sb, "    ");
                    } else {
                        sb.append(table.toString());
                    }

                    sb.append("  }").append(System.lineSeparator());
                });
            }

            sb.append("}");
            return sb.toString();
        });
    }

    private static class TableIds {
        private final boolean tableIdCaseInsensitive;
        private final Set<TableId> values;

        public TableIds(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new HashSet();
        }

        public void add(TableId tableId) {
            this.values.add(this.toLowerCaseIfNeeded(tableId));
        }

        public Set<TableId> toSet() {
            return new HashSet(this.values);
        }

        public void clear() {
            this.values.clear();
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return this.tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }
    }

    private static class TablesById {
        private final boolean tableIdCaseInsensitive;
        private final ConcurrentMap<TableId, Table> values;

        public TablesById(boolean tableIdCaseInsensitive) {
            this.tableIdCaseInsensitive = tableIdCaseInsensitive;
            this.values = new ConcurrentHashMap();
        }

        public Set<TableId> ids() {
            return this.values.keySet();
        }

        boolean isEmpty() {
            return this.values.isEmpty();
        }

        public void putAll(Tables.TablesById tablesByTableId) {
            if (this.tableIdCaseInsensitive) {
                tablesByTableId.values.entrySet().forEach((e) -> {
                    this.put(((TableId)e.getKey()).toLowercase(), (Table)e.getValue());
                });
            } else {
                this.values.putAll(tablesByTableId.values);
            }

        }

        public Table remove(TableId tableId) {
            return (Table)this.values.remove(this.toLowerCaseIfNeeded(tableId));
        }

        public Table get(TableId tableId) {
            return (Table)this.values.get(this.toLowerCaseIfNeeded(tableId));
        }

        public Table put(TableId tableId, Table updated) {
            return (Table)this.values.put(this.toLowerCaseIfNeeded(tableId), updated);
        }

        int size() {
            return this.values.size();
        }

        void forEach(BiConsumer<? super TableId, ? super Table> action) {
            this.values.forEach(action);
        }

        Set<Entry<TableId, Table>> entrySet() {
            return this.values.entrySet();
        }

        void clear() {
            this.values.clear();
        }

        private TableId toLowerCaseIfNeeded(TableId tableId) {
            return this.tableIdCaseInsensitive ? tableId.toLowercase() : tableId;
        }

        public int hashCode() {
            return this.values.hashCode();
        }

        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null) {
                return false;
            } else if (this.getClass() != obj.getClass()) {
                return false;
            } else {
                Tables.TablesById other = (Tables.TablesById)obj;
                return this.values.equals(other.values);
            }
        }
    }

    @FunctionalInterface
    public interface ColumnNameFilter {
        boolean matches(String var1, String var2, String var3, String var4);
    }

    public static class ColumnNameFilterFactory {
        public ColumnNameFilterFactory() {
        }

        public static Tables.ColumnNameFilter createExcludeListFilter(String fullyQualifiedColumnNames, ColumnFilterMode columnFilterMode) {
            Predicate<ColumnId> delegate = Predicates.excludes(fullyQualifiedColumnNames, ColumnId::toString);
            return (catalogName, schemaName, tableName, columnName) -> {
                return delegate.test(new ColumnId(columnFilterMode.getTableIdForFilter(catalogName, schemaName, tableName), columnName));
            };
        }

        public static Tables.ColumnNameFilter createIncludeListFilter(String fullyQualifiedColumnNames, ColumnFilterMode columnFilterMode) {
            Predicate<ColumnId> delegate = Predicates.includes(fullyQualifiedColumnNames, ColumnId::toString);
            return (catalogName, schemaName, tableName, columnName) -> {
                return delegate.test(new ColumnId(columnFilterMode.getTableIdForFilter(catalogName, schemaName, tableName), columnName));
            };
        }
    }

    @FunctionalInterface
    public interface TableFilter extends DataCollectionFilter<TableId> {
        boolean isIncluded(TableId var1);

        static Tables.TableFilter fromPredicate(Predicate<TableId> predicate) {
            return (t) -> {
                return predicate.test(t);
            };
        }

        static Tables.TableFilter includeAll() {
            return (t) -> {
                return true;
            };
        }
    }
}
