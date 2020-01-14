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
                Buckets = new Dictionary<int, BucketInfo>();
            }

            public bool Equals(ref KeyEnvelope k1, ref KeyEnvelope k2)
            {
                if (k1.Type != k2.Type)
                    return false;

                switch (k1.Type)
                {
                    case KeyTypes.Content: return _keyComparer.Equals(ref k1.Content, ref k2.Content);
                    case KeyTypes.IndexBucket: return k1.BucketId == k2.BucketId;
                }

                return false;
            }


            public long GetHashCode64(ref KeyEnvelope k)
            {
                switch (k.Type)
                {
                    case KeyTypes.Content: return _keyComparer.GetHashCode64(ref k.Content);
                    case KeyTypes.IndexBucket: return k.BucketId;
                }
                return 0;
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

            public Dictionary<int, BucketInfo> Buckets;

        }

        private bool TryGetBucket(int bucketId, out BucketInfo bucketInfo)
        {
            if (_keyComparer.Buckets.TryGetValue(bucketId, out bucketInfo))
                return true;

            if (TryReadIndexBucket(bucketId, out bucketInfo))
                return true;

            bucketInfo = null;
            return false;
        }


        private bool TryReadIndexBucket(int bucketId, out BucketInfo bucketInfo)
        {
            bucketInfo = null;

            KeyEnvelope keyEnvelope = new KeyEnvelope(bucketId);

            ValueEnvelope inputEnvelope = default;
            ValueEnvelope outputEnvelope = default;

            Status status = Status.ERROR;


            status = ExecuteGet(ref keyEnvelope, ref inputEnvelope, ref outputEnvelope);

            switch (status)
            {
                case Status.ERROR:
                    throw new Exception($"Index Bucket read error => {bucketId}");

                case Status.NOTFOUND:
                    return false;

                case Status.OK:
                    bucketInfo = outputEnvelope.Bucket;
                    return true;
            }

            return false;
        }

    }
}
