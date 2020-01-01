using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace FasterDictionary.Tests
{
    [CollectionDefinition("Non-Parallel Collection", DisableParallelization = true)]
    public class RecoverTests : IDisposable
    {


        static string DataDirectoryPath;
        static RecoverTests()
        {
            DataDirectoryPath = Path.Combine(Path.GetTempPath(), "FasterDictionary.Tests");
        }

        public RecoverTests()
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
        //[InlineData(1)]
        //[InlineData(100)]
        //[InlineData(10_000)]
        [InlineData(100_000, 2)]
        [InlineData(10_000_000, 163684)]
        public async Task AddGet(int loops, int step)
        {
            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(GetOptions(nameof(AddGet))))
            {
                for (var i = 0; i < loops; i++)
                    dictionary.Upsert(i, (i + 1).ToString()).Forget();

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
