using FASTER.core;
using System;
using System.Collections.Generic;
using System.Text;

namespace FasterDictionary
{




    public partial class FasterDictionary<TKey, TValue>
    {

        class KeyComparerAdapter : IFasterEqualityComparer<KeyEnvelope>, IEqualityComparer<TKey>
        {
            static int BucketCount = 512;
            static bool IsKeyValueType;
            static KeyComparerAdapter()
            {
                IsKeyValueType = Utilities.IsValueType<TKey>();
            }

            IFasterEqualityComparer<TKey> _keyComparer;
            

            public KeyComparerAdapter(IFasterEqualityComparer<TKey> keyComparer)
            {
                _keyComparer = keyComparer;
            }

            public bool Equals(ref KeyEnvelope k1, ref KeyEnvelope k2)
            {
                return _keyComparer.Equals(ref k1.Content, ref k2.Content); 
            }


            public long GetHashCode64(ref KeyEnvelope k)
            {
                return _keyComparer.GetHashCode64(ref k.Content);
            }

            public int GetBucketId(TKey key)
            {
                return (int)(_keyComparer.GetHashCode64(ref key) % BucketCount) + 1;
            }

            public bool Equals(TKey x, TKey y)
            {
                return _keyComparer.Equals(ref x, ref y);
            }

            public int GetHashCode(TKey obj)
            {
                return (int)_keyComparer.GetHashCode64(ref obj);
            }

        }


        public void Compact(long untilAddress)
        {
            if (allocator is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                var functions = new LogVariableCompactFunctions(varLen);
                var variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };

                Compact(functions, untilAddress, variableLengthStructSettings);
            }
            else
            {
                Compact(new LogCompactFunctions(), untilAddress, null);
            }
        }

        private void Compact<T>(T functions, long untilAddress, VariableLengthStructSettings<Key, Value> variableLengthStructSettings)
            where T : IFunctions<Key, Value, Input, Output, Context>
        {
            var fhtSession = fht.NewSession();

            var originalUntilAddress = untilAddress;

            var tempKv = new FasterKV<Key, Value, Input, Output, Context, T>
                (fht.IndexSize, functions, new LogSettings(), comparer: fht.Comparer, variableLengthStructSettings: variableLengthStructSettings);
            var tempKvSession = tempKv.NewSession();

            using (var iter1 = fht.Log.Scan(fht.Log.BeginAddress, untilAddress))
            {
                while (iter1.GetNext(out RecordInfo recordInfo))
                {
                    ref var key = ref iter1.GetKey();
                    ref var value = ref iter1.GetValue();

                    if (recordInfo.Tombstone)
                        tempKvSession.Delete(ref key, default, 0);
                    else
                        tempKvSession.Upsert(ref key, ref value, default, 0);
                }
            }

            // TODO: Scan until SafeReadOnlyAddress
            long scanUntil = untilAddress;
            LogScanForValidity(ref untilAddress, ref scanUntil, ref tempKvSession);

            // Make sure key wasn't inserted between SafeReadOnlyAddress and TailAddress

            using (var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress))
            {
                while (iter3.GetNext(out RecordInfo recordInfo))
                {
                    ref var key = ref iter3.GetKey();
                    ref var value = ref iter3.GetValue();

                    if (!recordInfo.Tombstone)
                    {
                        if (fhtSession.ContainsKeyInMemory(ref key, scanUntil) == Status.NOTFOUND)
                            fhtSession.Upsert(ref key, ref value, default, 0);
                    }
                    if (scanUntil < fht.Log.SafeReadOnlyAddress)
                    {
                        LogScanForValidity(ref untilAddress, ref scanUntil, ref tempKvSession);
                    }
                }
            }
            fhtSession.Dispose();
            tempKvSession.Dispose();
            tempKv.Dispose();

            ShiftBeginAddress(originalUntilAddress);
        }



    }
}
