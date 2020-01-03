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
        //[InlineData(2, 1)]
        //[InlineData(100, 1)]
        //[InlineData(10_000, 1)]
        //[InlineData(1_000_000, 2)]
        [InlineData(10_000_000, 20)]
        public async Task AddRestartGetValues(int loops, int step)
        {
            var options = GetOptions($"{nameof(AddRestartGetValues)}-{loops}");

            options.DeleteOnClose = false;

            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(options))
            {
                for (var i = 0; i < loops; i++)
                    await dictionary.Upsert(i, (i + 1).ToString());

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 1).ToString(), result.Value);
                }

                await dictionary.Save();
            }

            using (var dictionary = new FasterDictionary<int, string>(options))
            {
                for (var i = 0; i < loops; i++)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 1).ToString(), result.Value);
                }

                for (var i = 0; i < loops; i++)
                    await dictionary.Upsert(i, (i + 2).ToString());

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 2).ToString(), result.Value);
                }

                await dictionary.Save();
            }

            options.DeleteOnClose = true;

            using (var dictionary = new FasterDictionary<int, string>(options))
            {
                for (var i = 0; i < loops; i += step)
                {
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal((i + 2).ToString(), result.Value);
                }
            }


        }

        private static FasterDictionary<int, string>.Options GetOptions( string directoryName, bool deleteOnClose = true)
        {
            return new FasterDictionary<int, string>.Options()
            {
                DictionaryName = directoryName,
                PersistDirectoryPath = DataDirectoryPath,
                DeleteOnClose = deleteOnClose,
                CheckPointType = FASTER.core.CheckpointType.FoldOver,
                Logger = new FasterLogger()
            };
        }

    }
}
