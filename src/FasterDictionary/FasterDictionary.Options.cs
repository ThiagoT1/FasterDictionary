﻿using FASTER.core;
using System;
using System.Collections.Generic;
using System.Text;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        public struct Options
        {
            //static Options FasterDefaults = new Options()
            //{
            //    SegmentSize = MemorySizes.GB1,
            //    MemorySize = MemorySizes.MB512,
            //    PageSize = MemorySizes.MB32
            //};

            public static readonly Options Default = new Options()
            {
                MemorySize = MemorySizes.MB32,
                PageSize = MemorySizes.MB8,
                SegmentSize = MemorySizes.MB16
            };


            public string PersistDirectoryPath;
            public string DictionaryName;

            
            public MemorySizes SegmentSize;
            public MemorySizes PageSize;
            public MemorySizes MemorySize;

            public ILogger Logger;

            public bool DeleteOnClose;

            public CheckpointType CheckPointType;
        }

        Options _options;
        KeyComparerAdapter _keyComparer;

    }
}
