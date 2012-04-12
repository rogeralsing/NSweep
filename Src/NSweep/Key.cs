using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Globalization;

namespace NSweep
{
    public struct Key
    {
        byte[] bytes;
        int length;

        public byte[] GetBytes()
        {
            byte[] buffer = new byte[length];
            Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
            return buffer;
        }

        public static Key Composite(params Key[] composite)
        {
            Key i;
            i.length = 0;
            for (int c = 0, l = composite.Length; c < l; c++)
                i.length += composite[c].length;
            i.bytes = new byte[i.length];
            int offset = 0;
            for (int c = 0, l = composite.Length; c < l; c++)
            {
                Buffer.BlockCopy(composite[c].bytes, 0, i.bytes, offset, Math.Min(composite[c].bytes.Length, composite[c].length));
                offset += composite[c].length;
            }
            return i;
        }

        // Micro optimization
        public static Key Composite(Key a, Key b)
        {
            Key i;
            i.length = a.length + b.length;
            i.bytes = new byte[i.length];
            Buffer.BlockCopy(a.bytes, 0, i.bytes, 0, Math.Min(a.bytes.Length, a.length));
            Buffer.BlockCopy(b.bytes, 0, i.bytes, a.length, Math.Min(b.bytes.Length, b.length));
            return i;
        }

        public static Key Desc(int data)
        {
            Key k = data;
            InvertBits(k.bytes);
            return k;
        }

        public static Key Desc(long data)
        {
            Key k = data;
            InvertBits(k.bytes);
            return k;
        }

        public static Key Desc(DateTime data)
        {
            Key k = data;
            InvertBits(k.bytes);
            return k;
        }

        public static Key Fixed(string data, int length, CompareInfo collation, Direction direction = Direction.Ascending)
        {
            return Fixed(data, length, collation, CompareOptions.None);
        }

        public static Key Fixed(string data, int length, CompareInfo collation, CompareOptions collationOptions, Direction direction = Direction.Ascending)
        {
            Key i;
            var tmp = collation.GetSortKey(data, collationOptions).KeyData;

            if (direction == Direction.Descending)
                InvertBits(tmp);

            i.bytes = tmp;
            i.length = length * 6; // Assumes 2 bytes per character is enough, may not be true for all UTF-16 characters
            return i;
        }

        public static Key Fixed(byte[] data, int length)
        {
            Key i;
            i.bytes = data;
            i.length = length;
            return i;
        }

        public static implicit operator byte[](Key i)
        {
            if (i.bytes.Length != i.length) return Key.Composite(i).bytes;
            return i.bytes;
        }

        public static implicit operator Key(int data)
        {
            Key i;
            var tmp = unchecked((uint)(data ^ (1 << 31)));
            i.bytes = BitConverter.GetBytes(tmp);
            EnsureBigEndian(i.bytes);
            i.length = sizeof(int);
            return i;
        }

        public static implicit operator Key(long data)
        {
            Key i;
            var tmp = unchecked((ulong)(data ^ (1 << 63)));
            i.bytes = BitConverter.GetBytes(tmp);
            EnsureBigEndian(i.bytes);
            i.length = sizeof(long);
            return i;
        }

        private static void EnsureBigEndian(byte[] a)
        {
            if (BitConverter.IsLittleEndian) Array.Reverse(a);
        }

        public static implicit operator Key(DateTime data)
        {
            return data.ToUniversalTime().Ticks;
        }

        public static Key Fixed(System.Data.SqlTypes.SqlString data, int length)
        {
            return Fixed(data.Value, length, data.CompareInfo, System.Data.SqlTypes.SqlString.CompareOptionsFromSqlCompareOptions(data.SqlCompareOptions));
        }

        private static readonly int[] sqlServerGuidSortOrder = new int[] { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };

        public static implicit operator Key(System.Data.SqlTypes.SqlGuid data)
        {
            // SQL Server Uses a special sorting algorithm for Guids
            var temp = data.ToByteArray();
            Key i;
            i.bytes = new byte[16];
            i.length = 16;
            for (var c = 0; c < 16; c++) i.bytes[c] = temp[sqlServerGuidSortOrder[c]];
            return i;
        }

        private static void InvertBits(byte[] b)
        {
            for (int i = 0; i < b.Length; i++)
                b[i] ^= 0xff;
        }

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        static extern int memcmp(byte[] b1, byte[] b2, int count);

        public int CompareTo(Key other)
        {
            return Compare(this, other);
        }

        public static int Compare(Key a, Key b)
        {
         //   if (a.length != b.length) throw new Exception("Cannot compare keys of different length.");
            int r = memcmp(a.bytes, b.bytes, Math.Min(a.bytes.Length, b.bytes.Length));
            if (r != 0) return r;
            int al = a.bytes.Length, bl = b.bytes.Length;
            if (al < bl && IsNotEmpty(b.bytes, al, bl)) return -1;
            if (al > bl && IsNotEmpty(a.bytes, bl, al)) return 1;
            return 0;
        }

        private static bool IsNotEmpty(byte[] buffer, int offset, int offsetEnd)
        {
            for (int i = offset; i < offsetEnd; i++)
                if (buffer[i] != (byte)0)
                    return true;
            return false;
        }
    }

    public enum Direction
    {
        Ascending,
        Descending
    }
}
