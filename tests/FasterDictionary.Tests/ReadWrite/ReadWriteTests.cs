using FasterDictionary.Tests.Util;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace FasterDictionary.Tests
{
    [CollectionDefinition("Non-Parallel Collection", DisableParallelization = true)]
    public class ReadWriteTests : IDisposable
    {


        static string DataDirectoryPath;
        static ReadWriteTests()
        {
            DataDirectoryPath = Path.Combine(Path.GetTempPath(), "FasterDictionary.Tests", "ReadWriteTests");
        }

        public ReadWriteTests()
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
        [InlineData(1_000_000, 4)]
        [InlineData(3_000_000, 31)]
        public async Task AddGet(int loops, int step)
        {
            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(TestHelper.GetKeyComparer<int>(), GetOptions($"{nameof(AddGet)}-{loops}")))
            {
                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(i, (i + 1).ToString()).Dismiss();

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 1).ToString(), result.Value);
                }

                result = await dictionary.TryGet(loops);
                Assert.False(result.Found);
            }
        }


        [Theory]
        [InlineData(2)]
        [InlineData(100)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        [InlineData(1_000_000)]
        [InlineData(1_500_000)]
        //        [InlineData(5_000_000)]
        public async Task AddIterate(int loops)
        {
            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(TestHelper.GetKeyComparer<int>(), GetOptions($"{nameof(AddIterate)}-{loops}")))
            {
                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(i, (i + 1).ToString()).Dismiss();

                await dictionary.Ping();

                var count = 0;
                await foreach (var entry in dictionary)
                {
                    count++;
                    Assert.Equal((entry.Key + 1).ToString(), entry.Value);
                }

                result = await dictionary.TryGet(loops);
                Assert.False(result.Found);

                Assert.Equal(loops, count);
            }
        }


        [Theory]
        [InlineData(2, 1)]
        [InlineData(100, 1)]
        [InlineData(10_000, 1)]
        [InlineData(1_000_000, 4)]
        [InlineData(3_000_000, 31)]
        public async Task AddUpdateGet(int loops, int step)
        {
            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(TestHelper.GetKeyComparer<int>(), GetOptions($"{nameof(AddGet)}-{loops}")))
            {
                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(i, (i + 1).ToString()).Dismiss();

                await dictionary.Ping();

                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(i, (i + 10).ToString()).Dismiss();

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 10).ToString(), result.Value);
                }

                await dictionary.Ping();


                result = await dictionary.TryGet(loops);
                Assert.False(result.Found);
            }
        }

        [Theory]
        [InlineData(2, 1)]
        [InlineData(100, 1)]
        [InlineData(10_000, 1)]
        [InlineData(1_000_000, 4)]
        [InlineData(3_000_000, 31)]
        public async Task AddGetRemove(int loops, int step)
        {
            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(TestHelper.GetKeyComparer<int>(), GetOptions($"{nameof(AddGetRemove)}-{loops}")))
            {
                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(i, (i + 1).ToString()).Dismiss();

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 1).ToString(), result.Value);
                }

                for (var i = 0; i < loops; i += step)
                    if (i % 5 != 0)
                        await dictionary.Remove(i);


                for (var i = 0; i < loops; i += step)
                {

                    result = await dictionary.TryGet(i);
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

        private static FasterDictionary<int, string>.Options GetOptions(string directoryName, bool deleteOnClose = true)
        {
            return new FasterDictionary<int, string>.Options()
            {
                DictionaryName = directoryName,
                PersistDirectoryPath = DataDirectoryPath,
                DeleteOnClose = deleteOnClose
            };
        }

    }
}
