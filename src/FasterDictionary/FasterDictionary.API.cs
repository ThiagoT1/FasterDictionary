using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace FasterDictionary
{
    public enum MemorySizes
    {
        MB4 = 22,
        MB8 = MB4 + 1,
        MB16 = MB8 + 1,
        MB32 = MB16 + 1,
        MB64 = MB32 + 1,
        MB128 = MB64 + 1,
        MB256 = MB128 + 1,
        MB512 = MB256 + 1,
        MB1024 = MB512 + 1,
        GB1 = MB1024,
        GB2 = GB1 + 1
    }

    

    public partial class FasterDictionary<TKey, TValue> : IDisposable
    {
       
        public interface IKeyComparer
        {

        }

        public ValueTask<ReadResult> Ping()
        {
            return Enqueue(new Job(JobTypes.Ping));
        }

        public ValueTask<ReadResult> Save()
        {
            return Enqueue(new Job(JobTypes.Save));
        }

        public ValueTask<ReadResult> TryGet(TKey key)
        {
            return Enqueue(new Job(key, JobTypes.Get));
        }

        public ValueTask<ReadResult> Upsert(TKey key, TValue value)
        {
            return Enqueue(new Job(key, value, JobTypes.Upsert));
        }

        public ValueTask<ReadResult> Remove(TKey key)
        {
            return Enqueue(new Job(key, JobTypes.Remove));
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            Enqueue(new Job(JobTypes.Dispose));
        }
    }
}
