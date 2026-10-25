using System;

namespace QuickFix.Fields;

/// <summary>
/// Base class for all field types
/// </summary>
/// <typeparam name="T">Internal storage type</typeparam>
public abstract class FieldBase<T> : IField
    where T : notnull
{
    /// Set _stringVal (val only) and _stringField (tag=val)

    private T _value;
    private int _tag;
    private bool _changed;
    private string _stringVal; // field value (not tag) formatted for fix
    private string? _stringField; // field string e.g. tag=val (no SOH); built on first ToStringField()

    /// <summary>
    /// Constructs a new field with the specified tag and value
    /// </summary>
    /// <param name="tag">the FIX tag number</param>
    /// <param name="value">the value of the field</param>
    public FieldBase(int tag, T value)
    {
        _tag = tag;
        _value = value;
        _changed = true;

        // We can't set these properly here because makeString() is abstract.
        // See https://stackoverflow.com/a/119543/650475
        _stringVal = "";
        _stringField = null;
    }

    public T Value
    {
        get => _value;
        set
        {
            _value = value;
            _changed = true;
        }
    }

    //-----------------------------------
    // IField implementations

    /// <summary>
    /// the FIX tag number
    /// </summary>
    public int Tag
    {
        get => _tag;
        set
        {
            _tag = value;
            _changed = true;
        }
    }

    /// <summary>
    /// returns full fix string (e.g. "tag=val")
    /// </summary>
    public string ToStringField()
    {
        if (_changed)
            MakeStringFields();
        return _stringField ??= Tag + "=" + _stringVal;
    }

    /// <summary>
    /// returns field value (not tag) formatted for fix
    /// </summary>
    public override string ToString()
    {
        if (_changed)
            MakeStringFields();
        return _stringVal;
    }

    /// <summary>
    /// length of formatted field (including the trailing SOH) e.g. tag=val\001
    /// </summary>
    public int GetLength()
    {
        if (_changed)
            MakeStringFields();
        if (_tag < 0 || !CharEncoding.IsAsciiCompatible)
            return CharEncoding.SelectedEncoding.GetByteCount(ToStringField()) + 1; // +1 for SOH

        // Avoid building "tag=val": the tag digits and '=' are one byte each
        int tagLength = 1;
        for (int t = _tag; t >= 10; t /= 10)
            tagLength++;
        return tagLength + 1 + CharEncoding.ByteCount(_stringVal) + 1; // +1 for '=', +1 for SOH
    }

    /// <summary>
    /// Sum of bytes; used in calculating checksum
    /// </summary>
    public int GetTotal()
    {
        if (_changed)
            MakeStringFields();
        if (_tag < 0 || !CharEncoding.IsAsciiCompatible)
            return CharEncoding.ByteSum(ToStringField()) + 1; // +1 for SOH

        // Avoid building "tag=val": the tag digits and '=' are one byte each
        int sum = 0;
        int t = _tag;
        do
        {
            sum += '0' + t % 10;
            t /= 10;
        } while (t > 0);

        return sum + '=' + CharEncoding.ByteSum(_stringVal) + 1; // +1 for SOH
    }

    //-----------------------------------
    // C# Equals/HashCode overrides

    /// <summary>
    /// Value equality test
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object? obj)
    {
        if (obj is null || GetType() != obj.GetType())
            return false;

        FieldBase<T> f = (FieldBase<T>)obj;
        return this.Tag == f.Tag && this.Value.Equals(f.Value);
    }

    public override int GetHashCode()
    {
        return Tag ^ Value.GetHashCode();
    }

    /// <summary>
    /// Convert underlying field value to fix-ready string
    /// </summary>
    /// <returns></returns>
    protected abstract string MakeString();

    /// <summary>
    /// Set _stringVal (val only) and clear _stringField (tag=val), which is rebuilt on demand
    /// </summary>
    private void MakeStringFields()
    {
        _stringVal = MakeString();
        _stringField = null;
        _changed = false;
    }

    [Obsolete("Use capitalized ToStringField() instead. This function will be removed in 1.16.")]
    public string toStringField() { return ToStringField(); }
    [Obsolete("Use capitalized GetLength() instead. This function will be removed in 1.16.")]
    public int getLength() { return GetLength(); }
    [Obsolete("Use capitalized GetTotal() instead. This function will be removed in 1.16.")]
    public int getTotal() { return GetTotal(); }
    [Obsolete("Renamed to 'Value'. This attribute name will be removed in 1.16.")]
    public T Obj
    {
        get => _value;
        set
        {
            _value = value;
            _changed = true;
        }
    }
    [Obsolete("Use Value getter property. This function will be removed in 1.16.")]
    public T getValue() { return Value; }
    [Obsolete("Use Value setter property. This function will be removed in 1.16.")]
    public void setValue(T v) { Value = v; }
}

