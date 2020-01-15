using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FasterDictionary
{




    public partial class FasterDictionary<TKey, TValue> : IAsyncEnumerable<KeyValuePair<TKey, TValue>>
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



        public IAsyncEnumerator<KeyValuePair<TKey, TValue>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new AsyncEnumerator(this);
        }
        class AsyncEnumerator : IAsyncEnumerator<KeyValuePair<TKey, TValue>>
        {
            FasterDictionary<TKey, TValue> _fasterDictionary;

            public ValueTask StartTask { get; }

            public AsyncEnumerator(FasterDictionary<TKey, TValue> fasterDictionary)
            {
                _fasterDictionary = fasterDictionary;
                StartTask = _fasterDictionary.AquireIterator();
            }

            KeyValuePair<TKey, TValue> _current;
            public KeyValuePair<TKey, TValue> Current => _current;

            public ValueTask DisposeAsync()
            {
                return _fasterDictionary.ReleaseIterator();
            }

            public async ValueTask<bool> MoveNextAsync()
            {
                await StartTask;

                _current = default;
                var readResult = await _fasterDictionary.Iterate();
                if (!readResult.Found)
                    return false;

                _current = new KeyValuePair<TKey, TValue>(readResult.Key, readResult.Value);
                return true;
            }
        }

        private class LogCompactFunctions : IFunctions<KeyEnvelope, byte, byte, byte, Context>
        {
            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
            public void ConcurrentReader(ref KeyEnvelope key, ref byte input, ref byte value, ref byte dst) { }
            public bool ConcurrentWriter(ref KeyEnvelope key, ref byte src, ref byte dst) { dst = src; return true; }
            public void CopyUpdater(ref KeyEnvelope key, ref byte input, ref byte oldValue, ref byte newValue) { }
            public void InitialUpdater(ref KeyEnvelope key, ref byte input, ref byte value) { }
            public bool InPlaceUpdater(ref KeyEnvelope key, ref byte input, ref byte value) { return true; }
            public void ReadCompletionCallback(ref KeyEnvelope key, ref byte input, ref byte output, Context ctx, Status status) { }
            public void RMWCompletionCallback(ref KeyEnvelope key, ref byte input, Context ctx, Status status) { }
            public void SingleReader(ref KeyEnvelope key, ref byte input, ref byte value, ref byte dst) { }
            public void SingleWriter(ref KeyEnvelope key, ref byte src, ref byte dst) { dst = src; }
            public void UpsertCompletionCallback(ref KeyEnvelope key, ref byte value, Context ctx) { }
            public void DeleteCompletionCallback(ref KeyEnvelope key, Context ctx) { }
        }


        //public void Compact(long untilAddress)
        //{
        //    if (allocator is VariableLengthBlittableAllocator<Key, Value> varLen)
        //    {
        //        var functions = new LogVariableCompactFunctions(varLen);
        //        var variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
        //        {
        //            keyLength = varLen.KeyLength,
        //            valueLength = varLen.ValueLength,
        //        };

        //        Compact(functions, untilAddress, variableLengthStructSettings);
        //    }
        //    else
        //    {
        //        Compact(new LogCompactFunctions(), untilAddress, null);
        //    }
        //}

        

        class IterationContextState : IDisposable
        {
            public LogCompactFunctions Functions;
            public long UntilAddress;
            public FasterKV<KeyEnvelope, byte, byte, byte, Context, LogCompactFunctions> TempKV;

            public ClientSession<KeyEnvelope, byte, byte, byte, Context, LogCompactFunctions> KVSession;

            public IFasterScanIterator<KeyEnvelope, byte> KeyIteration;

            public void Dispose()
            {
                KVSession.Kill();
                TempKV.Kill();
            }
        }

        IterationContextState _iterationState;

        private void ServeReleaseIterator(Job job)
        {
            _iterationState.Kill();
            _iterationState = null;
            job.Complete(false);
        }

        private void ServeIteration(Job job)
        {
            RecordInfo recordInfo;
            try
            {
                tryAgain:

                if (!_iterationState.KeyIteration.GetNext(out recordInfo))
                {
                    job.Complete(false);
                    return;
                }

                if (recordInfo.Tombstone)
                    goto tryAgain;

                ref var key = ref _iterationState.KeyIteration.GetKey();
                
                job.Key = key.Content;

                ServeGetContent(job);
                
                if (!job.AsyncOp.ValueTaskOfT.Result.Found)
                    _options.Logger.Info($"Iterator Failed: NOTFOUND => {JsonConvert.SerializeObject(key)}");
                
            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void ServeAquireIterator(Job job)
        {
            if (_iterationState != null)
            {
                job.Complete(new Exception("Iteration already in progress"));
                return;
            }
            try
            {
                _iterationState = new IterationContextState()
                {
                    Functions = new LogCompactFunctions(),
                    UntilAddress = KV.Log.TailAddress
                };

                _iterationState.TempKV = new FasterKV<KeyEnvelope, byte, byte, byte, Context, LogCompactFunctions>
                    (KV.IndexSize, _iterationState.Functions, new LogSettings(), comparer: KV.Comparer, variableLengthStructSettings: null);

                _iterationState.KVSession = _iterationState.TempKV.NewSession();

                using (var iter1 = KV.Log.Scan(KV.Log.BeginAddress, KV.Log.TailAddress))
                {
                    byte stub = 0;
                    while (iter1.GetNext(out RecordInfo recordInfo))
                    {
                        ref var key = ref iter1.GetKey();

                        if (recordInfo.Tombstone)
                            _iterationState.KVSession.Delete(ref key, default, 0);
                        else
                            _iterationState.KVSession.Upsert(ref key, ref stub, default, 0);
                    }
                }

                // TODO: Scan until SafeReadOnlyAddress
                long scanUntil = _iterationState.UntilAddress;
                LogScanForValidity(ref _iterationState.UntilAddress, ref scanUntil, ref _iterationState.KVSession);

                // Make sure key wasn't inserted between SafeReadOnlyAddress and TailAddress

                _iterationState.KeyIteration = _iterationState.TempKV.Log.Scan
                (
                    _iterationState.TempKV.Log.BeginAddress,
                    _iterationState.TempKV.Log.TailAddress
                );


                job.Complete(true);
            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void LogScanForValidity<T>(ref long untilAddress, ref long scanUntil,
            ref ClientSession<KeyEnvelope, byte, byte, byte, Context, T> tempKvSession)
        where T : IFunctions<KeyEnvelope, byte, byte, byte, Context>
        {
            while (scanUntil < KV.Log.SafeReadOnlyAddress)
            {
                untilAddress = scanUntil;
                scanUntil = KV.Log.SafeReadOnlyAddress;
                using var iter2 = KV.Log.Scan(untilAddress, scanUntil);
                while (iter2.GetNext(out RecordInfo recordInfo))
                {
                    ref var key = ref iter2.GetKey();

                    tempKvSession.Delete(ref key, default, 0);
                }
            }
        }


    }
}
