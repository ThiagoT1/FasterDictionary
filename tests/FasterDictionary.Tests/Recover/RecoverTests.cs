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
        [InlineData(1)]
        //[InlineData(100)]
        //[InlineData(10_000)]
        //[InlineData(1_000_000)]
        public async Task AddGet(int iterations)
        {
            var dictionary = new FasterDictionary<int, string>();
            await dictionary.Upsert(1, "1");
            var result = await dictionary.TryGet(1);

        }

        
    }
}
