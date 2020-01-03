using System;
using System.IO;
using System.Linq;
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
        [InlineData(226, 1)]
        [InlineData(227, 1)]
        //[InlineData(174_763, 1)]
        //[InlineData(1_000_000, 2)]
        //[InlineData(10_000_000, 20)]
        public async Task AddRestartGetValues(int loops, int step)
        {
            var options = GetOptions($"{nameof(AddRestartGetValues)}-{loops}");

            options.DeleteOnClose = false;

            FasterDictionary<int, string>.ReadResult result;
            using (var dictionary = new FasterDictionary<int, string>(options))
            {
                for (var i = 0; i < loops; i++)
                {
                    var guid = GetGuid(i);
                    await dictionary.Upsert(i, guid);
                }

                await dictionary.Ping();

                for (var i = 0; i < loops; i += step)
                {
                    var guid = GetGuid(i);
                    result = await dictionary.TryGet(i);
                    Assert.True(result.Found);
                    Assert.Equal(guid.ToString(), result.Value);
                }

                await dictionary.Save();
            }

            for (var k = 0; k < 3; k++)
                using (var dictionary = new FasterDictionary<int, string>(options))
                {
                    await dictionary.Ping();

                    for (var i = 0; i < loops; i++)
                    {
                        var guid = GetGuid(i);
                        result = await dictionary.TryGet(i);
                        Assert.True(result.Found);
                        Assert.Equal(guid, result.Value);
                    }
                }

        }

        private static string GetGuid(int i, int count = 1000)
        {
            return string.Join('-', Enumerable.Repeat(new Guid(i, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).ToString(), count).ToArray());
        }

        private static FasterDictionary<int, string>.Options GetOptions(string directoryName, bool deleteOnClose = true)
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
