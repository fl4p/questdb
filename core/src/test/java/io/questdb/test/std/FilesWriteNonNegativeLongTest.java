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

package io.questdb.test.std;

import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class FilesWriteNonNegativeLongTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testFailureFacadeOverrideReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                try {
                    Assert.assertTrue(Files.allocate(fd, Long.BYTES));
                    Assert.assertTrue(Files.writeNonNegativeLong(fd, 0, 0xCAFEBABEL));

                    FailureFileFacade failingFf = new FailureFileFacade(FilesFacadeImpl.INSTANCE);
                    failingFf.setToFailAfter(1);

                    Assert.assertFalse(failingFf.writeNonNegativeLong(fd, 0, 0xDEADL));
                    Assert.assertEquals(0xCAFEBABEL, Files.readNonNegativeLong(fd, 0));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testOverwrite() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                try {
                    Assert.assertTrue(Files.allocate(fd, 16));
                    Assert.assertTrue(Files.writeNonNegativeLong(fd, 8, 0x1111_1111_1111_1111L));
                    Assert.assertTrue(Files.writeNonNegativeLong(fd, 8, 0x2222_2222_2222_2222L));
                    Assert.assertEquals(0x2222_2222_2222_2222L, Files.readNonNegativeLong(fd, 8));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testRoundtripAtLargeOffset() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                long size2Gb = (2L << 30) + 4096;
                try {
                    Assert.assertTrue(Files.truncate(fd, size2Gb));

                    long testValue = 0x1234_5678_90AB_CDEFL;
                    Assert.assertTrue(Files.writeNonNegativeLong(fd, size2Gb - 8, testValue));
                    Assert.assertEquals(testValue, Files.readNonNegativeLong(fd, size2Gb - 8));
                } finally {
                    Files.close(fd);
                    TestUtils.remove(path.$());
                }
            }
        });
    }

    @Test
    public void testWriteAndReadBack() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                try {
                    Assert.assertTrue(Files.allocate(fd, Long.BYTES));

                    Assert.assertTrue(Files.writeNonNegativeLong(fd, 0, 0xDEAD_BEEFL));
                    Assert.assertEquals(0xDEAD_BEEFL, Files.readNonNegativeLong(fd, 0));

                    Assert.assertTrue(Files.writeNonNegativeLong(fd, 0, Long.MAX_VALUE));
                    Assert.assertEquals(Long.MAX_VALUE, Files.readNonNegativeLong(fd, 0));

                    Assert.assertTrue(Files.writeNonNegativeLong(fd, 0, 0L));
                    Assert.assertEquals(0L, Files.readNonNegativeLong(fd, 0));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testWriteAtNonZeroOffset() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                try {
                    long fileLen = 2048L;
                    Assert.assertTrue(Files.allocate(fd, fileLen));

                    long zeroBuf = Unsafe.calloc(fileLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Assert.assertEquals(fileLen, Files.write(fd, zeroBuf, fileLen, 0));
                    } finally {
                        Unsafe.free(zeroBuf, fileLen, MemoryTag.NATIVE_DEFAULT);
                    }

                    long offset = 1024L;
                    long value = 0x0123_4567_89AB_CDEFL;
                    Assert.assertTrue(Files.writeNonNegativeLong(fd, offset, value));

                    Assert.assertEquals(value, Files.readNonNegativeLong(fd, offset));
                    Assert.assertEquals(0L, Files.readNonNegativeLong(fd, 0));
                    Assert.assertEquals(0L, Files.readNonNegativeLong(fd, fileLen - 8));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }

    @Test
    public void testWriteToClosedFdReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                Assert.assertTrue(Files.allocate(fd, Long.BYTES));
                Files.close(fd);

                Assert.assertFalse(Files.writeNonNegativeLong(fd, 0, 0xDEADL));
            }
        });
    }

    @Test
    public void testWriteToReadOnlyFdReturnsFalse() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long original = 0xABCD_EF01_2345_6789L;
                long fdrw = Files.openRW(path.$());
                try {
                    Assert.assertTrue(Files.allocate(fdrw, Long.BYTES));
                    Assert.assertTrue(Files.writeNonNegativeLong(fdrw, 0, original));
                } finally {
                    Files.close(fdrw);
                }

                long fdro = Files.openRO(path.$());
                try {
                    Assert.assertFalse(Files.writeNonNegativeLong(fdro, 0, 0xDEADL));
                    Assert.assertEquals(original, Files.readNonNegativeLong(fdro, 0));
                } finally {
                    Files.close(fdro);
                }
            }
        });
    }

    @Test
    public void testWriteValuesPairWithReadNonNegativeLong() throws Exception {
        assertMemoryLeak(() -> {
            File temp = temporaryFolder.newFile();
            try (Path path = new Path().of(temp.getAbsolutePath())) {
                long fd = Files.openRW(path.$());
                try {
                    Assert.assertTrue(Files.allocate(fd, Long.BYTES * 4L));

                    FilesFacade ff = FilesFacadeImpl.INSTANCE;
                    Assert.assertTrue(ff.writeNonNegativeLong(fd, 0, 0L));
                    Assert.assertTrue(ff.writeNonNegativeLong(fd, 8, 1L));
                    Assert.assertTrue(ff.writeNonNegativeLong(fd, 16, 0x7FFF_FFFF_FFFF_FFFFL));
                    Assert.assertTrue(ff.writeNonNegativeLong(fd, 24, 42L));

                    Assert.assertEquals(0L, ff.readNonNegativeLong(fd, 0));
                    Assert.assertEquals(1L, ff.readNonNegativeLong(fd, 8));
                    Assert.assertEquals(0x7FFF_FFFF_FFFF_FFFFL, ff.readNonNegativeLong(fd, 16));
                    Assert.assertEquals(42L, ff.readNonNegativeLong(fd, 24));
                } finally {
                    Files.close(fd);
                }
            }
        });
    }
}
