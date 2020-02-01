using FASTER.core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FasterDictionary
{




    public partial class FasterDictionary<TKey, TValue> : IAsyncEnumerable<KeyValuePair<TKey, TValue>>
    {

        public IAsyncEnumerable<TKey> AsKeysIterator() => new KeyIterator(this);

        class KeyIterator : IAsyncEnumerable<TKey>
        {
            FasterDictionary<TKey, TValue> _fasterDictionary;
            public KeyIterator(FasterDictionary<TKey, TValue> fasterDictionary)
            {
                _fasterDictionary = fasterDictionary;
            }

            public IAsyncEnumerator<TKey> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            {
                return new KeyAsyncEnumerator(_fasterDictionary);
            }

            class KeyAsyncEnumerator : IAsyncEnumerator<TKey>
            {
                FasterDictionary<TKey, TValue> _fasterDictionary;

                public ValueTask StartTask { get; }

                public KeyAsyncEnumerator(FasterDictionary<TKey, TValue> fasterDictionary)
                {
                    _fasterDictionary = fasterDictionary;
                    StartTask = _fasterDictionary.AquireIterator();
                }

                TKey _current;
                public TKey Current => _current;

                public ValueTask DisposeAsync()
                {
                    return _fasterDictionary.ReleaseIterator();
                }

                public async ValueTask<bool> MoveNextAsync()
                {
                    await StartTask;

                    _current = default;
                    var readResult = await _fasterDictionary.IteratePair();
                    if (!readResult.Found)
                        return false;

                    _current = readResult.Key;
                    return true;
                }
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
                var readResult = await _fasterDictionary.IteratePair();
                if (!readResult.Found)
                    return false;

                _current = new KeyValuePair<TKey, TValue>(readResult.Key, readResult.Value);
                return true;
            }
        }

        


        

        class IterationContextState : IDisposable
        {
            public Functions Functions;
            public long UntilAddress;
            public FasterKV<VariableEnvelope, VariableEnvelope, byte[], byte[], Context, Functions> TempKV;

            public ClientSession<VariableEnvelope, VariableEnvelope, byte[], byte[], Context, Functions> KVSession;

            public IFasterScanIterator<VariableEnvelope, VariableEnvelope> KeyIteration;

            public void Dispose()
            {
                KVSession.Kill();
                TempKV.Kill();
            }
        }

        IterationContextState _iterationState;

        private void ServeReleaseIterator(Job job)
        {
            _iterationState?.KVSession.Kill();
            _iterationState.Kill();
            _iterationState = null;
            job.Complete(false);
        }

        private void ServeKeyIteration(Job job)
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

                job.Key = key.To<TKey>();
                
                job.Complete(true);

            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void ServePairIteration(Job job)
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
                ref var value = ref _iterationState.KeyIteration.GetValue();

                job.Key = key.To<TKey>();

                job.Complete(true, value.To<TValue>());
                
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
                    Functions = new Functions(_options.Logger),
                    UntilAddress = KV.Log.TailAddress
                };

                var variableLengthStructSettings = new VariableLengthStructSettings<VariableEnvelope, VariableEnvelope>()
                {
                    keyLength = VariableEnvelope.Settings,
                    valueLength = VariableEnvelope.Settings
                };

                _iterationState.TempKV = new FasterKV<VariableEnvelope, VariableEnvelope, byte[], byte[], Context, Functions>
                    (KV.IndexSize, _iterationState.Functions, new LogSettings(), comparer: KV.Comparer, variableLengthStructSettings: variableLengthStructSettings);

                _iterationState.KVSession = _iterationState.TempKV.NewSession();

                using (var iter1 = KV.Log.Scan(KV.Log.BeginAddress, KV.Log.TailAddress))
                {

                    while (iter1.GetNext(out RecordInfo recordInfo))
                    {
                        ref var key = ref iter1.GetKey();
                        ref var value = ref iter1.GetValue();

                        _iterationState.KVSession.Delete(ref key, default, 0);

                        if (!recordInfo.Tombstone)
                            _iterationState.KVSession.Upsert(ref key, ref value, default, 0);
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
            ref ClientSession<VariableEnvelope, VariableEnvelope, byte[], byte[], Context, T> tempKvSession)
        where T : IFunctions<VariableEnvelope, VariableEnvelope, byte[], byte[], Context>
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
