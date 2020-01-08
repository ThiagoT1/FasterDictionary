using FASTER.core;
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
            DataDirectoryPath = Path.Combine(Path.GetTempPath(), "FasterDictionary.Tests", "RecoverTests");
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
        [InlineData(226, 1, CheckpointType.FoldOver)]  //OK
        [InlineData(227, 1, CheckpointType.FoldOver)]  //OK
        [InlineData(50_000, 1, CheckpointType.FoldOver)]  //OK
        
        [InlineData(2832, 1, CheckpointType.Snapshot)] //OK
        [InlineData(2833, 1, CheckpointType.Snapshot)] //OK
        [InlineData(50_000, 1, CheckpointType.Snapshot)] //OK
        public async Task AddRestartGetValues(int loops, int step, CheckpointType checkpointType)
        {
            var options = GetOptions($"{nameof(AddRestartGetValues)}-{loops}");

            options.CheckPointType = checkpointType;
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

        private static string GetGuid(int i, int count = 100)
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
                CheckPointType = FASTER.core.CheckpointType.Snapshot,
                Logger = new FasterLogger()
            };
        }

    }
}
