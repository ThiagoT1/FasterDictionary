using System;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        class Job
        {
            const bool ContinueAsync = true;
            const bool ContinueSync = false;

            public AsyncOperation<ReadResult> AsyncOp;

            public TKey Key;
            public TValue Input;
            public JobTypes JobType;

            public Job(TKey key, TValue input, JobTypes type)
            {
                AsyncOp = new AsyncOperation<ReadResult>(ContinueAsync);
                //TaskSource = new TaskCompletionSource<ReadResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                Key = key;
                Input = input;
                JobType = type;
            }

            public Job(TKey key, JobTypes type)
            {
                AsyncOp = new AsyncOperation<ReadResult>(ContinueAsync);
                Key = key;
                JobType = type;
            }

            public Job(JobTypes type)
            {
                AsyncOp = new AsyncOperation<ReadResult>(ContinueAsync);
                JobType = type;
            }

            public void Complete(bool found, TValue value = default) => AsyncOp?.TrySetResult(new ReadResult(found, Key, value));
            public void Complete(Exception e) => AsyncOp?.TrySetException(e);


        }

    }
}
