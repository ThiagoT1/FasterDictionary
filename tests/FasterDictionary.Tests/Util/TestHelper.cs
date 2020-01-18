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

        class StringArrayComparer : IFasterEqualityComparer<string[]>
        {
            public bool Equals(ref string[] k1, ref string[] k2)
            {
                if (k1 == null)
                    return k2 == null;

                if (k1.Length != k2.Length)
                    return false;

                for (var i = k1.Length - 1; i >= 0; i--)
                {
                    if (k1[i] != k2[i])
                        return false;
                }

                return true;

            }

            public long GetHashCode64(ref string[] k)
            {
                if (k == null)
                    return 0;

                int r = k.Length;

                for (var i = 0; i < k.Length; i++)
                    r += GetHashCode(ref k[i]);

                return r;
            }

            int GetHashCode(ref string str)
            {
                if (str == null)
                    return 1;

                int r = str.Length;

                for (var i = 0; i < str.Length; i++)
                    r = r * (byte)str[i] + (byte)str[i];


                return r;

            }
        }

        internal static IFasterEqualityComparer<T> GetKeyComparer<T>()
        {
            if (Utilities.IsValueType<T>())
            {
                return new ValueTypeComparer<T>();
            }

            if (typeof(T) == typeof(string[]))
            {
                return (IFasterEqualityComparer<T>)new StringArrayComparer();
            }

            throw new Exception("WTF");
        }
    }
}
