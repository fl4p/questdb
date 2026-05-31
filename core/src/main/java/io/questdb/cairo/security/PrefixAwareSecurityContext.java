/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.security;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * Security context for a named user restricted to tables whose name starts with
 * an allowed prefix, optionally read-only. Built by
 * {@link PrefixAwareSecurityContextFactory} from an {@code AclEntry}.
 * <p>
 * Enforcement model:
 * <ul>
 *   <li>any operation naming a table outside the prefix is denied (throws),</li>
 *   <li>when read-only, every write/DDL operation is denied,</li>
 *   <li>{@link #canViewTable(TableToken)} returns whether the table is in the
 *       prefix and is used by the catalogue listing cursors to hide tables the
 *       user may not see,</li>
 *   <li>database-wide and admin operations are denied.</li>
 * </ul>
 * An empty or null prefix means "all tables" (no name restriction), still
 * subject to the read-only flag.
 */
public class PrefixAwareSecurityContext implements SecurityContext {
    private final String prefix;
    private final String principal;
    private final boolean readOnly;

    public PrefixAwareSecurityContext(@NotNull String principal, String prefix, boolean readOnly) {
        this.principal = principal;
        this.prefix = prefix == null || prefix.isEmpty() ? null : prefix;
        this.readOnly = readOnly;
    }

    @Override
    public void authorizeAlterMatViewSetRefreshLimit(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterMatViewSetRefreshType(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableAddColumn(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableAddIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableAlterColumnCache(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableAlterColumnType(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableAlterSymbolCapacity(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableAttachPartition(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableConvertPartitionToNative(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableConvertPartitionToParquet(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableDedupDisable(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableDedupEnable(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableDetachPartition(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableDropColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableDropIndex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableDropPartition(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableRenameColumn(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableSetParam(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableSetParquetSettings(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterTableSetType(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeAlterView(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeCopyCancel(SecurityContext cancellingSecurityContext) {
        denyAdmin();
    }

    @Override
    public void authorizeDatabaseBackup() {
        denyAdmin();
    }

    @Override
    public void authorizeDatabaseSnapshot() {
        denyAdmin();
    }

    @Override
    public void authorizeHttp() {
    }

    @Override
    public void authorizeInsert(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeLineTcp() {
    }

    @Override
    public void authorizeMatViewCreate(CharSequence matViewName) {
        checkCreate(matViewName);
    }

    @Override
    public void authorizeMatViewDrop(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeMatViewRefresh(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizePGWire() {
    }

    @Override
    public void authorizeResumeWal(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeSelect(ViewDefinition viewDefinition) {
    }

    @Override
    public void authorizeSelect(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkVisible(tableToken);
    }

    @Override
    public void authorizeSelectOnAnyColumn(TableToken tableToken) {
        checkVisible(tableToken);
    }

    @Override
    public void authorizeSettings() {
        denyAdmin();
    }

    @Override
    public void authorizeSqlEngineAdmin() {
        denyAdmin();
    }

    @Override
    public void authorizeSystemAdmin() {
        denyAdmin();
    }

    @Override
    public void authorizeTableCreate(CharSequence tableName) {
        checkCreate(tableName);
    }

    @Override
    public void authorizeTableDrop(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeTableReindex(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeTableRename(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeTableTruncate(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeTableUpdate(TableToken tableToken, @NotNull ObjList<CharSequence> columnNames) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeTableVacuum(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeViewCompile(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public void authorizeViewCreate(CharSequence viewName) {
        checkCreate(viewName);
    }

    @Override
    public void authorizeViewDrop(TableToken tableToken) {
        checkWrite(tableToken);
    }

    @Override
    public boolean canViewTable(TableToken tableToken) {
        return inPrefix(tableToken);
    }

    @Override
    public void checkEntityEnabled() {
    }

    @Override
    public CharSequence getPrincipal() {
        return principal;
    }

    @Override
    public boolean isQueryCancellationAllowed() {
        return false;
    }

    @Override
    public boolean isSystemAdmin() {
        return false;
    }

    private void checkCreate(CharSequence name) {
        if (readOnly) {
            throw CairoException.authorization().put("Write permission denied").setCacheable(true);
        }
        // A prefix-scoped user may only create objects whose name falls within its prefix;
        // otherwise it could squat names in another tenant's namespace.
        if (prefix != null && !Chars.startsWith(name, prefix)) {
            throw CairoException.authorization().put("Access denied [table=").put(name).put(']').setCacheable(true);
        }
    }

    private void checkVisible(TableToken tableToken) {
        if (!inPrefix(tableToken)) {
            throw CairoException.authorization().put("Access denied [table=").put(tableToken.getTableName()).put(']').setCacheable(true);
        }
    }

    private void checkWrite(TableToken tableToken) {
        if (readOnly) {
            throw CairoException.authorization().put("Write permission denied").setCacheable(true);
        }
        checkVisible(tableToken);
    }

    private void denyAdmin() {
        throw CairoException.authorization().put("Access denied").setCacheable(true);
    }

    private boolean inPrefix(TableToken tableToken) {
        return prefix == null || tableToken.getTableName().startsWith(prefix);
    }
}
