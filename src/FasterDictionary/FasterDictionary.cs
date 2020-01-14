﻿using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {



        public FasterDictionary(IFasterEqualityComparer<TKey> keyComparer, Options options = default)
        {
            if (keyComparer == null)
                throw new ArgumentNullException(nameof(keyComparer));

            _keyComparer = new KeyComparerAdapter(keyComparer);
            _options = Options.Default;
            if (options.PersistDirectoryPath != null)
            {
                _options.DeleteOnClose = options.DeleteOnClose;
                _options.DictionaryName = options.DictionaryName;

                _options.Logger = options.Logger;
                _options.PersistDirectoryPath = options.PersistDirectoryPath;

                if (options.CheckPointType != CheckpointType.Snapshot)
                    _options.CheckPointType = options.CheckPointType;

                if (options.MemorySize != 0)
                    _options.MemorySize = options.MemorySize;

                if (options.PageSize != 0)
                    _options.PageSize = options.PageSize;

                if (options.SegmentSize != 0)
                    _options.SegmentSize = options.SegmentSize;
            }

            JobQueue = Channel.CreateUnbounded<Job>(new UnboundedChannelOptions()
            {
                AllowSynchronousContinuations = true,
                SingleReader = true,
                SingleWriter = false

            });

            StartConsumer();
        }

        Channel<Job> JobQueue;
        Task JobConsumerTask;

        private void StartConsumer()
        {
            JobConsumerTask = Task.Factory.StartNew(ConsumeJobs, TaskCreationOptions.LongRunning);
        }

        private async Task ConsumeJobs()
        {
            var functions = new Functions(_options.Logger);

            var checkpointDir = Path.Combine(_options.PersistDirectoryPath, "CheckPoints");
            var logDir = Path.Combine(_options.PersistDirectoryPath, "Logs");

            Directory.CreateDirectory(logDir);
            Directory.CreateDirectory(checkpointDir);

            var indexLogPath = Path.Combine(logDir, $"{_options.DictionaryName}-index.log");
            var objectLogPath = Path.Combine(logDir, $"{_options.DictionaryName}-object.log");

            IndexLog = Devices.CreateLogDevice(indexLogPath, true, _options.DeleteOnClose, -1, true);
            ObjectLog = Devices.CreateLogDevice(objectLogPath, true, _options.DeleteOnClose, -1, true);

            UnsafeContext = new Context();

            Log = new LogSettings
            {
                LogDevice = IndexLog,
                ObjectLogDevice = ObjectLog,
                SegmentSizeBits = (int)_options.SegmentSize,
                PageSizeBits = (int)_options.PageSize,
                MemorySizeBits = (int)_options.MemorySize
            };

            var checkpointSettings = new CheckpointSettings()
            {
                CheckpointDir = checkpointDir,
                CheckPointType = _options.CheckPointType
            };

            KV = new FasterKV
                <KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context, Functions>(
                    1L << 20, functions,
                    Log,
                    checkpointSettings,
                    new SerializerSettings<KeyEnvelope, ValueEnvelope>
                    {
                        keySerializer = () => new KeySerializer(),
                        valueSerializer = () => new ValueSerializer()
                    },
                    _keyComparer
                );

            var logCount = Directory.GetDirectories(checkpointDir).Length;
            if (logCount > 0)
                KV.Recover();

            KVSession = KV.NewSession();

            var reader = JobQueue.Reader;

            while (await reader.WaitToReadAsync())
            {
                while (reader.TryRead(out Job item))
                {
                    ServeJob(item);
                    if (item.JobType == JobTypes.Dispose)
                        return;
                }



            }
        }

        long serialNum = 0;



        private long GetSerialNum()
        {
            return serialNum++;
        }

        private void ServeJob(Job job)
        {
            switch (job.JobType)
            {
                case JobTypes.Upsert:
                    ServeUpsert(job);
                    break;

                case JobTypes.Get:
                    ServeGetContent(job);
                    break;

                case JobTypes.Remove:
                    ServeRemove(job);
                    break;

                case JobTypes.Ping:
                    ServePing(job);
                    break;

                case JobTypes.Save:
                    Task.Run(async () => await ServeSave(job)).Wait();
                    break;

                case JobTypes.Dispose:
                    ServeDispose(job);
                    break;
            }


        }

        private async Task ServeSave(Job job)
        {
            try
            {
                //KV.Log.FlushAndEvict(true);

                KV.TakeFullCheckpoint(out Guid token);
                await KV.CompleteCheckpointAsync();

                job.Complete(false);

            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void ServePing(Job job)
        {
            job.Complete(false);
        }

        private void ServeDispose(Job job)
        {
            try
            {

                JobQueue.Writer.Complete();

                Log.LogDevice.Close();
                Log.ObjectLogDevice.Close();

                KVSession.Dispose();
                KV.Dispose();

                job.Complete(false);

            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private void ServeRemove(Job job)
        {
            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);
            ValueEnvelope outputEnvelope = default;

            Status status = Status.ERROR;
            try
            {
                status = KVSession.Delete(ref keyEnvelope, UnsafeContext, GetSerialNum());
                if (status == Status.PENDING)
                {
                    KVSession.CompletePending(true, true);
                    status = UnsafeContext.Consume(out outputEnvelope);
                }

                UpdateIndexBucket(job, true);

            }
            catch (Exception e)
            {
                job.Complete(e);
            }

            switch (status)
            {
                case Status.ERROR:
                    job.Complete(new Exception($"read error => {JsonConvert.SerializeObject(job.Key)}"));
                    break;
                case Status.NOTFOUND:
                    job.Complete(false);
                    break;
                case Status.OK:
                    job.Complete(true, default);
                    break;
            }
        }

        private void ServeGetContent(Job job)
        {
            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);

            ValueEnvelope inputEnvelope = default;
            ValueEnvelope outputEnvelope = default;

            Status status = Status.ERROR;

            try
            {
                status = ExecuteGet(ref keyEnvelope, ref inputEnvelope, ref outputEnvelope);

                switch (status)
                {
                    case Status.ERROR:
                        job.Complete(new Exception($"read error => {JsonConvert.SerializeObject(job.Key)}"));
                        break;
                    case Status.NOTFOUND:
                        job.Complete(false);
                        break;
                    case Status.OK:
                        job.Complete(true, outputEnvelope.Content);
                        break;
                }
            }
            catch (Exception e)
            {
                job.Complete(e);
            }
        }

        private Status ExecuteGet(ref KeyEnvelope keyEnvelope, ref ValueEnvelope inputEnvelope, ref ValueEnvelope outputEnvelope)
        {
            Status status = KVSession.Read(ref keyEnvelope, ref inputEnvelope, ref outputEnvelope, UnsafeContext, GetSerialNum());
            if (status == Status.PENDING)
            {
                KVSession.CompletePending(true, true);
                status = UnsafeContext.Consume(out outputEnvelope);
            }

            return status;
        }

        private void ServeUpsert(Job job)
        {

            KeyEnvelope keyEnvelope = new KeyEnvelope(job.Key);
            ValueEnvelope valueEnvelope = new ValueEnvelope(job.Input);
            try
            {
                KVSession.Upsert(ref keyEnvelope, ref valueEnvelope, Context.Empty, GetSerialNum());
                UpdateIndexBucket(job);
            }
            catch (Exception e)
            {
                job.Complete(e);
            }
            finally
            {
                job.Complete(true, job.Input);
            }
        }

        private void UpdateIndexBucket(Job job, bool remove = false)
        {
            var bucketId = _keyComparer.GetBucketId(job.Key);

            if (!TryGetBucket(bucketId, out BucketInfo bucketInfo))
            {
                bucketInfo = new BucketInfo()
                {
                    Id = bucketId,
                    Keys = new System.Collections.Generic.HashSet<TKey>(_keyComparer)
                };
            }

            bool changed = false;

            if (remove)
            {
                changed = bucketInfo.Remove(job.Key);
            }
            else
            {
                changed = bucketInfo.Add(job.Key);
            }

            if (changed)
                SaveBucket(bucketId, bucketInfo);
        }

        private void SaveBucket(int bucketId, BucketInfo bucketInfo)
        {
            var keyEnvelope = new KeyEnvelope(bucketId);
            var valueEnvelope = new ValueEnvelope(bucketInfo);
            KVSession.Upsert(ref keyEnvelope, ref valueEnvelope, Context.Empty, GetSerialNum());
            _keyComparer.Buckets[bucketId] = bucketInfo;
        }

        enum JobTypes
        {
            None = 0,
            Upsert = 1,
            Remove = 2,
            Get = 3,
            Ping = 4,
            Save = 5,
            Dispose = 9
        }
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

            public void Complete(bool found, TValue value = default) => AsyncOp?.TrySetResult(new ReadResult(found, value));
            public void Complete(Exception e) => AsyncOp?.TrySetException(e);
        }

        public readonly struct ReadResult
        {
            public readonly TValue Value;
            public readonly bool Found;


            public ReadResult(bool found, TValue value = default)
            {
                Value = value;
                Found = found;
            }
        }


        private ValueTask<ReadResult> Enqueue(Job job)
        {
            JobQueue.Writer.TryWrite(job);
            if (job.AsyncOp == null)
                return new ValueTask<ReadResult>(default(ReadResult));
            return job.AsyncOp.ValueTaskOfT;
        }

    }
}
