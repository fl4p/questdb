/*+*****************************************************************************
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

package io.questdb.cutlass.influxdb;

import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * The result of translating a single InfluxQL statement. It carries either a
 * QuestDB SQL string to execute (with column-role metadata describing how to
 * shape the cursor into InfluxDB series JSON), a synthesized fixed response
 * (for {@code SHOW DATABASES} / {@code SHOW RETENTION POLICIES}), or a
 * per-statement error message (e.g. when the measurement does not exist).
 * <p>
 * For a SELECT, the authored cursor column order is always
 * {@code [timestamp, tag..., value...]}, so the role indices below are simple
 * positional ranges. For SHOW forms there is no timestamp and no tags; the
 * value columns are the cursor columns relabeled to the InfluxDB convention.
 */
public class TranslatedQuery implements Mutable {
    public final ObjList<String> synthColumns = new ObjList<>();
    public final ObjList<String> synthRowJson = new ObjList<>();
    public final IntList tagCols = new IntList();
    public final ObjList<String> tagLabels = new ObjList<>();
    public final IntList valueCols = new IntList();
    public final ObjList<String> valueLabels = new ObjList<>();
    // non-null when the statement parsed but cannot run (e.g. missing measurement);
    // reported per-statement at HTTP 200
    public String statementError;
    // when true, the response is fixed and needs no SQL execution
    public boolean synthesized;
    public String seriesName;
    public CharSequence sql;
    // cursor index of the timestamp column, or -1 for SHOW forms
    public int timeCol = -1;

    @Override
    public void clear() {
        sql = null;
        seriesName = null;
        statementError = null;
        synthesized = false;
        timeCol = -1;
        tagCols.clear();
        tagLabels.clear();
        valueCols.clear();
        valueLabels.clear();
        synthColumns.clear();
        synthRowJson.clear();
    }
}
