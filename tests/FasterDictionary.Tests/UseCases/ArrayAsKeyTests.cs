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
    public class ArrayAsKeyTests : IDisposable
    {


        static string DataDirectoryPath;
        static ArrayAsKeyTests()
        {
            DataDirectoryPath = Path.Combine(Path.GetTempPath(), "FasterDictionary.Tests", "ArrayAsKeyTests");
        }

        public ArrayAsKeyTests()
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
        [InlineData(1_000_000, 50)]
        //[InlineData(2_000_000, 31)]
        public async Task AddGet(int loops, int step)
        {
            FasterDictionary<string[], string>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddGet)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], (i + 1).ToString()).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(keys[i]);
                    Assert.True(result.Found);
                    Assert.Equal((i + 1).ToString(), result.Value);
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
            FasterDictionary<string[], string>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddIterate)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], (i + 1).ToString()).Dismiss();
                }

                await dictionary.Ping();

                var count = 0;
                await foreach (var entry in dictionary)
                {
                    count++;
                    Assert.Equal((int.Parse(entry.Key[1]) + 1).ToString(), entry.Value);
                }

                result = await dictionary.TryGet(new[] { "A", loops.ToString() });
                Assert.False(result.Found);

                Assert.Equal(loops, count);
            }
        }


        [Theory(Timeout = 3600000)]
        [InlineData(2, 1)]
        [InlineData(100, 1)]
        [InlineData(10_000, 1)]
        [InlineData(50_000, 1)]
        [InlineData(500_000, 1)]
        [InlineData(1_000_000, 1)]
        public async Task AddUpdateGet(int loops, int step)
        {
            FasterDictionary<string[], string>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddGet)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { i.ToString() });
                    dictionary.Upsert(keys[i], (i + 1).ToString()).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(keys[i], (i + 10).ToString()).Dismiss();

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(keys[i]);
                    Assert.True(result.Found);
                    Assert.Equal((i + 10).ToString(), result.Value);
                }

                await dictionary.Ping();


                result = await dictionary.TryGet(new[] { loops.ToString() });
                Assert.False(result.Found);
            }
        }

        [Theory(Timeout = 900000)]
        [InlineData(2)]
        [InlineData(100)]
        [InlineData(10_000)]
        [InlineData(50_000)]
        [InlineData(500_000)]
        [InlineData(1_000_000)]
        public async Task AddUpdateIterate(int loops)
        {
            FasterDictionary<string[], string>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddIterate)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { i.ToString() });
                    dictionary.Upsert(keys[i], (i + 1).ToString()).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(keys[i], (i + 10).ToString()).Dismiss();

                await dictionary.Ping();

                var count = 0;
                await foreach (var entry in dictionary)
                {
                    count++;
                    Assert.Equal((int.Parse(entry.Key[0]) + 10).ToString(), entry.Value);
                }

                result = await dictionary.TryGet(new[] { loops.ToString() });
                Assert.False(result.Found);

                Assert.Equal(loops, count);
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
            FasterDictionary<string[], string>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), GetOptions($"{nameof(AddGetRemove)}-{loops}")))
            {
                var keys = new List<string[]>();

                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], (i + 1).ToString()).Dismiss();
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(keys[i]);
                    Assert.True(result.Found);
                    Assert.Equal((i + 1).ToString(), result.Value);
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
                        Assert.Equal((i + 1).ToString(), result.Value);
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

            FasterDictionary<string[], string>.ReadResult result;
            using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), options))
            {
                for (var i = 0; i < loops; i++)
                {
                    keys.Add(new[] { "A", i.ToString() });
                    dictionary.Upsert(keys[i], (i + 1).ToString()).Dismiss();
                }

                await dictionary.Ping();

                await dictionary.Save();
            }

            for (var j = 0; j < rcovrCount; j++)
            {
                if (j == rcovrCount - 1)
                    options.DeleteOnClose = true;

                using (var dictionary = new FasterDictionary<string[], string>(TestHelper.GetKeyComparer<string[]>(), options))
                {
                    for (var k = 0; k < iteratCnt; k++)
                    {

                        var count = 0;
                        await foreach (var entry in dictionary)
                        {
                            count++;
                            Assert.Equal((int.Parse(entry.Key[1]) + 1).ToString(), entry.Value);
                        }

                        result = await dictionary.TryGet(new[] { "A", loops.ToString() });
                        Assert.False(result.Found);

                        Assert.Equal(loops, count);
                    }
                }
            }
        }

        private static FasterDictionary<string[], string>.Options GetOptions(string directoryName, bool deleteOnClose = true)
        {
            return new FasterDictionary<string[], string>.Options()
            {
                DictionaryName = directoryName,
                PersistDirectoryPath = DataDirectoryPath,
                DeleteOnClose = deleteOnClose,
                Logger = new FasterLogger<string[], string>()
            };
        }

    }
}
