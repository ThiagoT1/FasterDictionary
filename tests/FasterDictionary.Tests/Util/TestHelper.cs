using FASTER.core;
using System;
using System.Collections.Generic;
using System.Text;

namespace FasterDictionary.Tests.Util
{
    public static class TestHelper
    {
        class ValueTypeComparer<T> : IFasterEqualityComparer<T>
        {
            public bool Equals(ref T k1, ref T k2)
            {
                return Object.Equals(k1, k2);
            }

            public long GetHashCode64(ref T k)
            {
                return k.GetHashCode();
            }
        }
        internal static IFasterEqualityComparer<T> GetKeyComparer<T>()
        {
            if (Utilities.IsValueType<T>())
            {
                return new ValueTypeComparer<T>();
            }
            throw new Exception("WTF");
        }
    }
}
