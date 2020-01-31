using FASTER.core;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace FasterDictionary
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct VariableEnvelope
    {
        public static JsonSerializerOptions JsonOptions;

        internal static IVariableLengthStruct<VariableEnvelope> Settings;

        internal static IVariableLengthStruct<byte> ByteSettings;

        static VariableEnvelope()
        {
            Settings = new VarStructSettings();
            ByteSettings = new ByteStructSettings();
            JsonOptions = new JsonSerializerOptions()
            {
                IgnoreNullValues = true
            };
        }

        [FieldOffset(0)]
        public int Size;



        public void ToByteArray(ref byte[] dst)
        {
            dst = new byte[Size];
            byte* src = (byte*)Unsafe.AsPointer(ref this);
            src += sizeof(int);
            for (int i = 0; i < Size; i++)
            {
                dst[i] = *src;
                src++;
            }
        }

        public void CopyTo(ref VariableEnvelope dst)
        {
            var fulllength = Size * sizeof(byte) + sizeof(int);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref this),
                Unsafe.AsPointer(ref dst), fulllength, fulllength);
        }

        public unsafe T To<T>()
        {
            if (typeof(T) == typeof(byte[]))
            {
                byte[] dst = null;
                ToByteArray(ref dst);
                return (T)(object)dst;
            }

            byte* src = (byte*)Unsafe.AsPointer(ref this);
            src += intSize;
            var readOnly = new ReadOnlySpan<byte>(src, Size);
            return JsonSerializer.Deserialize<T>(readOnly, JsonOptions);
        }



        class VarStructSettings : IVariableLengthStruct<VariableEnvelope>
        {
            public int GetAverageLength()
            {
                return 64;
            }

            public int GetInitialLength<Input>(ref Input input)
            {
                return sizeof(int);
            }

            public int GetLength(ref VariableEnvelope t)
            {
                return sizeof(byte) * t.Size + sizeof(int);
            }
        }

        class ByteStructSettings : IVariableLengthStruct<byte>
        {
            public int GetAverageLength()
            {
                return 1;
            }

            public int GetInitialLength<Input>(ref Input input)
            {
                return 1;
            }

            public int GetLength(ref byte t)
            {
                return 1;
            }
        }

        public unsafe long GetHashCode64()
        {
            int result = 23;
            var src = new ReadOnlySpan<byte>(Unsafe.AsPointer(ref this), Size + intSize);

            var upperBound = src.Length;
            if (upperBound > 64)
                upperBound = 64;

            for (var i = 0; i < upperBound; i++)
                result *= (int)src[i] + 1;

            return result;
        }
        static int intSize = sizeof(int);
    }

    public struct VariableEnvelopeComparer : IFasterEqualityComparer<VariableEnvelope>
    {
        public long GetHashCode64(ref VariableEnvelope k)
        {
            return k.GetHashCode64();
        }

        static int intSize = sizeof(int);

        public unsafe bool Equals(ref VariableEnvelope k1, ref VariableEnvelope k2)
        {
            if (k1.Size != k2.Size)
                return false;

            var src = new ReadOnlySpan<byte>(Unsafe.AsPointer(ref k1), k1.Size + intSize);
            var dst = new ReadOnlySpan<byte>(Unsafe.AsPointer(ref k2), k2.Size + intSize);

            return src.SequenceEqual(dst);
        }
    }

    public partial class FasterDictionary<TKey, TValue>
    {



        public class Context
        {
            public static Context Empty = new Context();

            Status Status = Status.NOTFOUND;
            byte[] Value;

            internal void CompleteRead(ref Status status, ref byte[] output)
            {
                Status = status;
                Value = output;
            }

            internal Status Consume(out byte[] output)
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


        public class Functions : IFunctions<VariableEnvelope, VariableEnvelope, byte[], byte[], Context>
        {
            public Functions(ILogger logger)
            {
                Logger = logger;
            }

            public ILogger Logger { get; }

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            {
                Logger.Info(nameof(CheckpointCompletionCallback), $"SessionId: {sessionId}", $"CommitPoint: {JsonSerializer.Serialize(commitPoint.UntilSerialNo)} / {JsonSerializer.Serialize(commitPoint.ExcludedSerialNos)}");
            }

            public void ConcurrentReader(ref VariableEnvelope key, ref byte[] input, ref VariableEnvelope value, ref byte[] dst)
            {
                value.ToByteArray(ref dst);
            }

            public bool ConcurrentWriter(ref VariableEnvelope key, ref VariableEnvelope src, ref VariableEnvelope dst)
            {
                if (src.Size != dst.Size)
                    return false;
                
                src.CopyTo(ref dst);
                return true;
            }

            public bool InPlaceUpdater(ref VariableEnvelope key, ref byte[] input, ref VariableEnvelope value)
            {
                return true;
            }

            public void CopyUpdater(ref VariableEnvelope key, ref byte[] input, ref VariableEnvelope oldValue, ref VariableEnvelope newValue)
            {
                //newValue = oldValue;
            }

            public void DeleteCompletionCallback(ref VariableEnvelope key, Context ctx)
            {
                Logger.Trace(nameof(DeleteCompletionCallback), $"Key: {key.Size}");
            }

            public void InitialUpdater(ref VariableEnvelope key, ref byte[] input, ref VariableEnvelope value)
            {
                //value.Content = input.Content;
            }



            public void ReadCompletionCallback(ref VariableEnvelope key, ref byte[] input, ref byte[] output, Context ctx, Status status)
            {
                ctx.CompleteRead(ref status, ref output);
            }

            public void RMWCompletionCallback(ref VariableEnvelope key, ref byte[] input, Context ctx, Status status)
            {
                Logger.Trace(nameof(RMWCompletionCallback), $"Key: {key.Size}");
            }

            public void SingleReader(ref VariableEnvelope key, ref byte[] input, ref VariableEnvelope value, ref byte[] dst)
            {
                value.ToByteArray(ref dst);
            }

            public void SingleWriter(ref VariableEnvelope key, ref VariableEnvelope src, ref VariableEnvelope dst)
            {
                src.CopyTo(ref dst);
            }

            public void UpsertCompletionCallback(ref VariableEnvelope key, ref VariableEnvelope value, Context ctx)
            {
                Logger.Trace(nameof(UpsertCompletionCallback), $"Key: {key.Size}");
            }
        }

        LogSettings Log;
        IDevice IndexLog;
        //IDevice ObjectLog;

        FasterKV<VariableEnvelope, VariableEnvelope, byte[], byte[], Context, Functions> KV;

        ClientSession<VariableEnvelope, VariableEnvelope, byte[], byte[], Context, Functions> KVSession;

        Context UnsafeContext;


    }
}

