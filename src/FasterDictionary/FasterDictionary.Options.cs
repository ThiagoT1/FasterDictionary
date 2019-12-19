using System;
using System.Collections.Generic;
using System.Text;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        public struct Options
        {
            static Options FasterDefaults = new Options()
            {
                SegmentSize = MemorySizes.GB1,
                MemorySize = MemorySizes.MB512,
                PageSize = MemorySizes.MB32
            };

            public static readonly Options Default = new Options()
            {
                MemorySize = MemorySizes.MB128,
                PageSize = MemorySizes.MB16,
                SegmentSize = MemorySizes.MB64
            };


            public string PersistDirectoryPath;
            public IKeyComparer KeyComparer;
            public MemorySizes SegmentSize;
            public MemorySizes PageSize;
            public MemorySizes MemorySize;

        }

        Options _options;

    }
}
