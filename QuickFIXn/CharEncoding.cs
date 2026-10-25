using System;
using System.Buffers;

namespace QuickFix;

/// <summary>
/// Static class that manages the selected character encoding.
/// This impl is kind of a hack -- we're using a static class
/// so we don't have to alter a bunch of other classes to pass
/// a CharEncoding reference around.
/// </summary>
public static class CharEncoding
{
    public const string DefaultCharEncoding = "iso-8859-1";

    public static System.Text.Encoding SelectedEncoding { get; private set; }

    /// <summary>
    /// True if every ASCII char encodes to one byte of the same value in SelectedEncoding
    /// (iso-8859-1, utf-8, us-ascii, windows-1252, ...), which lets ByteCount/ByteSum skip encoding.
    /// </summary>
    private static bool _isAsciiCompatible;

    internal static bool IsAsciiCompatible => _isAsciiCompatible;

    static CharEncoding()
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
        Select(System.Text.Encoding.GetEncoding(DefaultCharEncoding));
    }

    public static void ResetToDefaultEncoding() => SetEncoding(DefaultCharEncoding);

    public static void SetEncoding(string encoding) => Select(System.Text.Encoding.GetEncoding(encoding));

    private static void Select(System.Text.Encoding encoding)
    {
        char[] ascii = new char[128];
        for (int i = 0; i < ascii.Length; i++)
            ascii[i] = (char)i;
        byte[] bytes = encoding.GetBytes(ascii);
        bool compatible = bytes.Length == ascii.Length;
        for (int i = 0; compatible && i < bytes.Length; i++)
            compatible = bytes[i] == i;

        _isAsciiCompatible = compatible;
        SelectedEncoding = encoding;
    }

    /// <summary>
    /// Number of bytes the chars encode to in SelectedEncoding
    /// </summary>
    internal static int ByteCount(ReadOnlySpan<char> chars)
    {
        if (_isAsciiCompatible && !chars.ContainsAnyExceptInRange((char)0, (char)0x7F))
            return chars.Length;
        return SelectedEncoding.GetByteCount(chars);
    }

    /// <summary>
    /// Sum of the bytes the chars encode to in SelectedEncoding (for FIX checksums)
    /// </summary>
    internal static int ByteSum(ReadOnlySpan<char> chars)
    {
        int sum = 0;
        if (_isAsciiCompatible && !chars.ContainsAnyExceptInRange((char)0, (char)0x7F))
        {
            foreach (char c in chars)
                sum += c;
            return sum;
        }

        using ValueDisposable _ = GetBytes(chars, out ReadOnlySpan<byte> bytes);
        foreach (byte b in bytes)
            sum += b;
        return sum;
    }

    /// <summary>
    /// Convert a string to a byte array using the current globally-set encoding.
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public static byte[] GetBytes(string data) => SelectedEncoding.GetBytes(data);

    /// <summary>
    /// Convert a string to a byte array using the current globally-set encoding.
    /// This function uses a shared byte-pool for efficiency.
    /// The return value needs to be disposed; `using` is recommended (see existing usages).
    /// This function is still internal; we're not sure if we want to make it part of the public interface.
    /// </summary>
    /// <param name="data"></param>
    /// <param name="bytes"></param>
    /// <returns></returns>
    internal static ValueDisposable GetBytes(ReadOnlySpan<char> data, out ReadOnlySpan<byte> bytes)
    {
        System.Text.Encoding encoding = SelectedEncoding;
        int byteCount = encoding.GetByteCount(data);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(byteCount);

        encoding.GetBytes(data, new Span<byte>(buffer, 0, byteCount));
        bytes = new ReadOnlySpan<byte>(buffer, 0, byteCount);

        return new ValueDisposable(buffer);
    }
}

internal readonly ref struct ValueDisposable(byte[]? bytes)
{
    public void Dispose()
    {
        if (bytes == null) return;
        if (bytes.Length > 0) ArrayPool<byte>.Shared.Return(bytes);
    }
}
