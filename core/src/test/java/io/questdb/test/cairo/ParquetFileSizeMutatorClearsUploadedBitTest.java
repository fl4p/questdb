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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Every primitive that writes the {@code parquetFileSize} slot — except
 * {@link TxWriter#setPartitionParquetFileSize} — must clear bit 63
 * (UPLOADED) by construction.
 */
public class ParquetFileSizeMutatorClearsUploadedBitTest extends AbstractCairoTest {

    @Test
    public void testSetPartitionParquetFileSizePreservesUploadedBit() throws Exception {
        // The bit-preserving mutator: set UPLOADED, call
        // setPartitionParquetFileSize with a different size, assert
        // UPLOADED is preserved AND the masked size matches the new value.
        TestUtils.assertMemoryLeak(() -> withTxWriter("mutPreserve", (tw, ts) -> {
            tw.setPartitionParquetFormat(ts, 4096L);
            tw.setPartitionUploaded(0, true);
            Assert.assertTrue(tw.isPartitionUploaded(0));
            Assert.assertEquals(4096L, tw.getPartitionParquetFileSize(0));

            tw.setPartitionParquetFileSize(0, 8192L);

            Assert.assertTrue("setPartitionParquetFileSize must preserve UPLOADED",
                    tw.isPartitionUploaded(0));
            Assert.assertEquals("setPartitionParquetFileSize must overwrite the size",
                    8192L, tw.getPartitionParquetFileSize(0));
        }));
    }

    @Test
    public void testSetPartitionParquetFormatClearsUploadedBit() throws Exception {
        // setPartitionParquetFormat is the other rewrite-path mutator
        // (DROP NATIVE -> switchNativePartitionWithParquet calls it).
        // Same invariant: a fresh non-negative size in the slot zeroes
        // bit 63.
        TestUtils.assertMemoryLeak(() -> withTxWriter("mutFormat", (tw, ts) -> {
            tw.setPartitionParquetFormat(ts, 4096L);
            tw.setPartitionUploaded(0, true);
            Assert.assertTrue(tw.isPartitionUploaded(0));

            // Call it again with a different size — stand-in for a
            // rewrite path that re-publishes the slot.
            tw.setPartitionParquetFormat(ts, 16_384L);

            Assert.assertFalse("setPartitionParquetFormat must clear UPLOADED on size rewrite",
                    tw.isPartitionUploaded(0));
            Assert.assertEquals("size must equal the new fileLength",
                    16_384L, tw.getPartitionParquetFileSize(0));
        }));
    }

    @Test
    public void testSetPartitionParquetGeneratedClearsUploadedBit() throws Exception {
        // Set UPLOADED on a parquet partition, then call the rewrite-path
        // mutator setPartitionParquetGenerated(idx, fileLength). The
        // mutator writes a raw non-negative long into the slot, which
        // clobbers bit 63 by construction. UPLOADED can never outlive
        // the bytes it claims were uploaded.
        TestUtils.assertMemoryLeak(() -> withTxWriter("mutGenerated", (tw, ts) -> {
            tw.setPartitionParquetFormat(ts, 4096L);
            tw.setPartitionUploaded(0, true);
            Assert.assertTrue("precondition: UPLOADED must be set", tw.isPartitionUploaded(0));

            tw.setPartitionParquetGenerated(0, 8192L);

            Assert.assertFalse("setPartitionParquetGenerated(idx, fileLength) must clear UPLOADED",
                    tw.isPartitionUploaded(0));
            Assert.assertEquals("size must equal the new fileLength",
                    8192L, tw.getPartitionParquetFileSize(0));
            Assert.assertTrue("parquet_generated must be set",
                    tw.isPartitionParquetGenerated(0));
        }));
    }

    private static void withTxWriter(String tableName, TxWriterAction action) throws Exception {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
        model.timestamp();
        AbstractCairoTest.create(model);
        try (Path path = new Path()) {
            final TableToken tableToken = engine.verifyTableName(tableName);
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
            try (TxWriter tw = new TxWriter(ff, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                final long ts = 0;
                tw.updatePartitionSizeByTimestamp(ts, 1);
                action.run(tw, ts);
            }
        }
    }

    @FunctionalInterface
    private interface TxWriterAction {
        void run(TxWriter tw, long partitionTimestamp) throws Exception;
    }
}
