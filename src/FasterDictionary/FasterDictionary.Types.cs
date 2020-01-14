using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace FasterDictionary
{
    public partial class FasterDictionary<TKey, TValue>
    {
        public enum KeyTypes : byte
        {
            Content = 0,
            IndexBucket = 1 
        }
        
        public struct KeyEnvelope
        {
            public TKey Content;
            public KeyTypes Type { get => BucketId == 0 ? KeyTypes.Content : KeyTypes.IndexBucket; }

            public int BucketId;

            public KeyEnvelope(TKey content)
            {
                Content = content;
                BucketId = 0;
            }

            public KeyEnvelope(int bucketId)
            {
                Content = default;
                BucketId = bucketId;
            }
        }


        public class BucketInfo
        {
            public static int PaddingSize = 128 * 1024;
            public BucketInfo()
            {
                Changed = true;
            }

            public int Id;
            public HashSet<TKey> Keys;


            byte[] _serializedContent;
            

            [JsonIgnore]
            public bool Changed { get; private set; }


            internal void EnsureSerialized()
            {
                if (Changed || _serializedContent == null)
                    _serializedContent = UTF8.GetBytes(JsonConvert.SerializeObject(this));
                
                Changed = false;
            }

            internal byte[] ConsumeSerialized()
            {
                EnsureSerialized();

                var s = _serializedContent;

                _serializedContent = null;

                return s;
            }

            internal bool Remove(TKey key)
            {
                var changed = Keys.Remove(key);
                if (changed)
                    Changed = true;
                return changed;
            }

            internal bool Add(TKey key)
            {
                var changed = Keys.Add(key);
                if (changed)
                    Changed = true;
                return changed;
            }
            
        }

        public struct ValueEnvelope
        {
            public TValue Content;
            
            public BucketInfo Bucket;

            public KeyTypes Type { get => Bucket == null ? KeyTypes.Content : KeyTypes.IndexBucket; }


            public ValueEnvelope(TValue content)
            {
                Content = content;
                Bucket = null;
                _serializedContent = null;
                SerializedSize = 0;  
            }

            public ValueEnvelope(BucketInfo bucket)
            {
                Content = default;
                Bucket = bucket;
                _serializedContent = null;
                SerializedSize = 0;
            }

            byte[] _serializedContent;

            public int SerializedSize { get; private set; }

            internal void EnsureSerializedSize()
            {
                if (Type == KeyTypes.Content)
                    EnsureSerialized();
                else
                    Bucket.EnsureSerializedSize();
            }

            internal void EnsureSerialized()
            {
                if (Type == KeyTypes.Content)
                {
                    if (_serializedContent == null)
                    {
                        if (Content is byte[] byteArray)
                        {
                            _serializedContent = byteArray;
                        }
                        else
                        {
                            _serializedContent = UTF8.GetBytes(JsonConvert.SerializeObject(Content));
                        }
                        SerializedSize = _serializedContent.Length;
                    }
                }
                else
                {
                    Bucket.EnsureSerialized();
                    SerializedSize = Bucket.SerializedSize;
                }
            }

            internal byte[] ConsumeSerialized()
            {
                
                if (Type == KeyTypes.Content)
                {
                    EnsureSerialized();

                    var s = _serializedContent;

                    _serializedContent = null;

                    return s;
                }
                else
                {
                    return Bucket.ConsumeSerialized();
                }
            }


            internal bool SameSize(ref ValueEnvelope valueEnvelope)
            {
                if (Type != valueEnvelope.Type)
                    throw new Exception("Cross type update!!");

                EnsureSerialized();
                valueEnvelope.EnsureSerialized();

                if (Type == KeyTypes.IndexBucket)
                {
                    var thisTileCount = Math.Ceiling((double)Bucket.SerializedSize / BucketInfo.PaddingSize);
                    var otherTileCount = Math.Ceiling((double)valueEnvelope.Bucket.SerializedSize / BucketInfo.PaddingSize);

                    if (otherTileCount == thisTileCount)
                        return true;
                }
                else
                {
                    return valueEnvelope.SerializedSize == SerializedSize;   
                }

                return false;
            }
        }

        public class Context
        {
            public static Context Empty = new Context();

            Status Status = Status.NOTFOUND;
            ValueEnvelope Value;

            internal void CompleteRead(ref Status status, ref ValueEnvelope output)
            {
                Status = status;
                Value = output;
            }

            internal Status Consume(out ValueEnvelope output)
            {
                output = Value;
                Value = default;
                var status = Status;
                Status = Status.NOTFOUND;
                return status;
            }
        }

        public interface ILogger
        {
            void Info(params string[] info);
            void Debug(params string[] info);
            void Trace(params string[] info);
        }


        public class Functions : IFunctions<KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context>
        {
            public Functions(ILogger logger)
            {
                Logger = logger;
            }

            public ILogger Logger { get; }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
                Logger.Info(nameof(CheckpointCompletionCallback), $"SessionId: {sessionId}", $"CommitPoint: {JsonConvert.SerializeObject(commitPoint)}");
            }

            public void ConcurrentReader(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value, ref ValueEnvelope dst)
            {
                dst = value;
            }

            public bool ConcurrentWriter(ref KeyEnvelope key, ref ValueEnvelope src, ref ValueEnvelope dst)
            {
                if (!src.SameSize(ref dst))
                    return false;
                
                dst = src;
                return true;
            }

            public bool InPlaceUpdater(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value)
            {
                if (!value.SameSize(ref input))
                    return false;

                value = input;
                return true;
            }

            public void CopyUpdater(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope oldValue, ref ValueEnvelope newValue)
            {
                newValue = oldValue;
            }

            public void DeleteCompletionCallback(ref KeyEnvelope key, Context ctx)
            {
                Logger.Trace(nameof(DeleteCompletionCallback), $"Key: {key.Content}");
            }

            public void InitialUpdater(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value)
            {
                value = input;
            }

            

            public void ReadCompletionCallback(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope output, Context ctx, Status status)
            {
                ctx.CompleteRead(ref status, ref output);
            }

            public void RMWCompletionCallback(ref KeyEnvelope key, ref ValueEnvelope input, Context ctx, Status status)
            {
                Logger.Trace(nameof(RMWCompletionCallback), $"Key: {key.Content}");
            }

            public void SingleReader(ref KeyEnvelope key, ref ValueEnvelope input, ref ValueEnvelope value, ref ValueEnvelope dst)
            {
                dst = value;
            }

            public void SingleWriter(ref KeyEnvelope key, ref ValueEnvelope src, ref ValueEnvelope dst)
            {
                dst = src;
            }

            public void UpsertCompletionCallback(ref KeyEnvelope key, ref ValueEnvelope value, Context ctx)
            {
                Logger.Trace(nameof(UpsertCompletionCallback), $"Key: {key.Content}");
            }
        }

        LogSettings Log;
        IDevice IndexLog;
        IDevice ObjectLog;

        FasterKV<KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context, Functions> KV;

        ClientSession<KeyEnvelope, ValueEnvelope, ValueEnvelope, ValueEnvelope, Context, Functions> KVSession;

        Context UnsafeContext;

        
    }
}

