using FASTER.core;
using FasterDictionary.Tests.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace FasterDictionary.Tests
{
    [CollectionDefinition("Non-Parallel Collection", DisableParallelization = true)]
    public class ArrayAsKeyByteAsValueTests : IDisposable
    {


        static string DataDirectoryPath;
        static ArrayAsKeyByteAsValueTests()
        {
            DataDirectoryPath = Path.Combine(Path.GetTempPath(), "FasterDictionary.Tests", "ArrayAsKeyByteAsValueTests");
        }

        public ArrayAsKeyByteAsValueTests()
        {
            ExcludeFiles();
        }

        public void Dispose()
        {
            ExcludeFiles();
        }

        private void ExcludeFiles()
        {
            if (Directory.Exists(DataDirectoryPath))
                Directory.Delete(DataDirectoryPath, true);
        }

        [Theory]
        [InlineData(2, 1)]
        [InlineData(100, 1)]
        [InlineData(10_000, 1)]
        [InlineData(1_000_000, 1)]
        //[InlineData(2_000_000, 31)]
        public async Task AddGet(int loops, int step)
        {
            FasterDictionary<string[], byte[]>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], byte[]>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddGet)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], new byte[] { (byte)(i + 1) }).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(keys[i]);
                    Assert.True(result.Found);
                    Assert.Equal((byte)(i + 1), result.Value[0]);
                }

                result = await dictionary.TryGet(new[] { "A", loops.ToString() });
                Assert.False(result.Found);
            }
        }


        [Theory]
        [InlineData(2)]
        [InlineData(100)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        [InlineData(1_000_000)]

        public async Task AddIterate(int loops)
        {
            FasterDictionary<string[], byte[]>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], byte[]>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddIterate)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], new byte[] { (byte)(i + 1) }).Dismiss();
                }

                await dictionary.Ping();

                var count = 0;
                await foreach (var entry in dictionary)
                {
                    count++;
                    Assert.Equal((byte)(int.Parse(entry.Key[1]) + 1), entry.Value[0]);
                }

                result = await dictionary.TryGet(new[] { "A", loops.ToString() });
                Assert.False(result.Found);

                Assert.Equal(loops, count);
            }
        }


        [Theory]
        [InlineData(2, 1)]
        [InlineData(100, 1)]
        [InlineData(10_000, 1)]
        [InlineData(1_000_000, 100)]
//        [InlineData(5_000_000, 31)]
        public async Task AddUpdateGet(int loops, int step)
        {
            FasterDictionary<string[], byte[]>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], byte[]>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddGet)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], new byte[] { (byte)(i + 1) }).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(keys[i], new byte[] { (byte)(i + 10) }).Dismiss();

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(keys[i]);
                    Assert.True(result.Found);
                    Assert.Equal((byte)(i + 10), result.Value[0]);
                }

                await dictionary.Ping();


                result = await dictionary.TryGet(new[] { "A", loops.ToString() });
                Assert.False(result.Found);
            }
        }

        [Theory]
        [InlineData(2, 1)]
        [InlineData(100, 1)]
        [InlineData(10_000, 1)]
        [InlineData(1_000_000, 50)]
//        [InlineData(5_000_000, 31)]
        public async Task AddGetRemove(int loops, int step)
        {
            FasterDictionary<string[], byte[]>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], byte[]>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddGetRemove)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], new byte[] { (byte)(i + 1) }).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(keys[i]);
                    Assert.True(result.Found);
                    Assert.Equal((byte)(i + 1), result.Value[0]);
                }

                for (var i = 0; i < loops; i += step)
                    if (i % 5 != 0)
                        await dictionary.Remove(keys[i]);


                for (var i = 0; i < loops; i += step)
                {

                    result = await dictionary.TryGet(keys[i]);
                    if (i % 5 == 0)
                    {
                        Assert.True(result.Found);
                        Assert.Equal((byte)(i + 1), result.Value[0]);
                    }
                    else
                    {
                        Assert.False(result.Found);
                    }
                }
            }
        }

        [Theory]


        [InlineData(1_000, 10, 1, CheckpointType.Snapshot)]
        [InlineData(10_000, 1, 100, CheckpointType.Snapshot)]

        [InlineData(100_000, 10, 1, CheckpointType.Snapshot)]
        [InlineData(100_000, 1, 25, CheckpointType.Snapshot)]


        [InlineData(1_000, 10, 1, CheckpointType.FoldOver)]
        [InlineData(10_000, 1, 100, CheckpointType.FoldOver)]

        [InlineData(100_000, 10, 1, CheckpointType.FoldOver)]
        [InlineData(100_000, 1, 25, CheckpointType.FoldOver)]

        public async Task AddRestartIterateMany(int loops, int rcovrCount, int iteratCnt, CheckpointType checkType)
        {
            var options = GetOptions($"{nameof(AddRestartIterateMany)}-{loops}");

            options.CheckPointType = checkType;
            options.DeleteOnClose = false;

            var keys = new List<string[]>();

            FasterDictionary<string[], byte[]>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], byte[]>(TestHelper.GetKeyComparer<string[]>(), options))
            {
                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], new byte[] { (byte)(i + 1) }).Dismiss();
                }

                await dictionary.Ping();

                await dictionary.Save();
            }

            for (var j = 0; j < rcovrCount; j++)
            {
                if (j == rcovrCount - 1)
                    options.DeleteOnClose = true;

                using (var dictionary = new FasterDictionary<string[], byte[]>(TestHelper.GetKeyComparer<string[]>(), options))
                {
                    for (var k = 0; k < iteratCnt; k++)
                    {

                        var count = 0;
                        await foreach (var entry in dictionary)
                        {
                            count++;
                            Assert.Equal((byte)(int.Parse(entry.Key[1]) + 1), entry.Value[0]);
                        }

                        result = await dictionary.TryGet(new[] { "A", loops.ToString() });
                        Assert.False(result.Found);

                        Assert.Equal(loops, count);
                    }
                }
            }
        }

        private static FasterDictionary<string[], byte[]>.Options GetOptions(string directoryName, bool deleteOnClose = true)
        {
            return new FasterDictionary<string[], byte[]>.Options()
            {
                DictionaryName = directoryName,
                PersistDirectoryPath = DataDirectoryPath,
                DeleteOnClose = deleteOnClose,
                Logger = new FasterLogger<string[], byte[]>()
            };
        }

    }
}
