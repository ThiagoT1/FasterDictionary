﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace FasterDictionary
{
    public static class Utilities
    {

        public static void Kill(this IDisposable disposable)
        {
            if (disposable == null)
                return;
            try
            {
                disposable.Dispose();
            }
            catch { }
        }
        public static bool IsValueType<T>()
        {
            return typeof(T).IsValueType;
        }
        public static void Dismiss(this Task task) { }
        public static void Dismiss<T>(this Task<T> task) { }

        public static void Dismiss(this ValueTask task) { }
        public static void Dismiss<T>(this ValueTask<T> task) { }
        public static void WriteInt32(this Stream target, int value)
        {
            target.WriteByte((byte)(value >> 24));
            target.WriteByte((byte)(value >> 16));
            target.WriteByte((byte)(value >> 8));
            target.WriteByte((byte)(value));
        }


        public static int ReadInt32(this Stream source)
        {
            var value = source.ReadByte() << 24;
            value += source.ReadByte() << 16;
            value += source.ReadByte() << 8;
            value += source.ReadByte();
            return value;
        }

        public static void WriteInt24(this Stream target, int value)
        {
            //target.WriteByte((byte)(value >> 24));
            target.WriteByte((byte)(value >> 16));
            target.WriteByte((byte)(value >> 8));
            target.WriteByte((byte)(value));
        }


        public static int ReadInt24(this Stream source)
        {
            var value = source.ReadByte() << 16;
            value += source.ReadByte() << 8;
            value += source.ReadByte();
            return value;
        }
    }
}
