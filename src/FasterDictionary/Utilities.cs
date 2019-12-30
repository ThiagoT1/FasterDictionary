﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FasterDictionary
{
    public static class Utilities
    {
        public static void WriteInt(this Stream target, int value)
        {
            target.WriteByte((byte)(value >> 24));
            target.WriteByte((byte)(value >> 16));
            target.WriteByte((byte)(value >> 8));
            target.WriteByte((byte)(value));
        }


        public static int ReadInt(this Stream source)
        {
            var value = source.ReadByte() << 24;
            value += source.ReadByte() << 16;
            value += source.ReadByte() << 8;
            value += source.ReadByte();
            return value;
        }
    }
}
