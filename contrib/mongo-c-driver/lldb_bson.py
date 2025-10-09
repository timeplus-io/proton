"""
This Python module is intended for use with the LLDB debugger.

To import using this module in an LLDB session, execute the following command
in LLDB::

    command script import <path-to-lldb_bson.py>

This may be useful to add as a "startup/init" command for an LLDB executor such
as VSCode.

From this point, all ``bson_t`` variables will show their full contents
recursively.

- Bonus: If using CodeLLDB, it is possible to add watch expressions to elements
  within a (nested) BSON document.

  To add a Python watch expression, prefix the watch string with ``/py``, and
  the remainder of the string will be evaluated in the LLDB Python interpreter
  rather than the LLDB evaluator. For example:

    /py 'hello, ' + 'world'

  will evaluate to a string "Hello, world".

  To reference a bson_t object, use the ``$variable_name`` syntax. Then, use the
  ``@`` operator with the special global ``bson`` object to walk through the
  document as if it were a regular Python object::

    /py $my_bson_t @ bson.topElement.path["string key"].array[3].name

  Only the leading ``bson`` is required in the path, since it provides the
  "magic" necessary to perform document traversal. For example, with the
  given document ``userdata``::

    {
        users: [
            {username: "joe", age: 41},
            {username: "alex", age: 33},
            {username: "jane', age: 29}
        ]
    }

  the "age" of "alex" can be watched with the following watch expression:

    /py $userdata @bson.users[1].age

"""

from __future__ import annotations

import enum
import functools
import hashlib
import json
import shlex
import struct
import traceback
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Generic,
    Iterable,
    NamedTuple,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import lldb  # pyright: ignore
from lldb import (  # pyright: ignore
    SBAddress,
    SBDebugger,
    SBError,
    SBFrame,
    SBProcess,
    SBSyntheticValueProvider,
    SBType,
    SBValue,
)

if TYPE_CHECKING:
    from typing_extensions import override
else:

    def override(f: T) -> T:
        return f


def print_errors(fn: FuncT) -> FuncT:
    """Exceptions from the decorated function will be printed, then re-raised"""

    @functools.wraps(fn)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except:
            e = traceback.format_exc()
            print(e)
            raise

    return cast("FuncT", _wrap)


@print_errors
def __lldb_init_module(debugger: SBDebugger, internal_dict: InternalDict):
    # Inject the global magic document traverser:
    internal_dict["bson"] = _BSONWalker()
    # Register types:
    for cls in _SyntheticMeta.synthetics:
        # The (regex of) the type that is handled by this class:
        ty = shlex.quote(cls.__typename__)
        if cls.__enable_synthetic__:
            # Register a synthetic child provider, generating the trees that can be expanded
            cmd = f"type synthetic add -l lldb_bson.{cls.__name__} -x '^{ty}$'"
            debugger.HandleCommand(cmd)
        if cls.__summary_str__ is not None:
            # Generate a simple summary string for this object
            quoted = cls.__summary_str__.replace("'", "\\'")
            cmd = f"type summary add --summary-string '{quoted}' -x '^{ty}$'"
            debugger.HandleCommand(cmd)
        if hasattr(cls, "__summary__"):
            # More complex: Call a Python function that will create the summary
            cmd = f"type summary add -F lldb_bson.{cls.__name__}.__summary__ -x '^{ty}$'"
            debugger.HandleCommand(cmd)

    # Render __bson_byte__ as "bytes with ASCII." __bson_byte__ is a
    # debug-only type generated on-the-fly in LLDB
    debugger.HandleCommand("type format add -f Y __bson_byte__")
    # Arrays of bytes as a sequence of hex values:
    debugger.HandleCommand(r"type summary add -s '${var[]%x}' -x '__bson_byte__\[[0-9]+\]'")

    print("lldb_bson is ready")


_ = __lldb_init_module  # Silence "unused function" warnings


FuncT = TypeVar("FuncT", bound=Callable[..., Any])
"Type of functions"
T = TypeVar("T")
"Unbounded invariant type parameter"
InternalDict = Dict[str, Any]
"Type of internal dictionaries, provided by LLDB"


ValueFactory = Callable[[], SBValue]
ChildItem = Union[
    Tuple[str, "str | int"], ValueFactory, Tuple[str, "str | int", "lldb.ValueFormatType|None", "SBType|None"]
]


class _SyntheticMeta(type):
    """
    Metaclass that handles subclassing of SyntheticDisplayBase. Does basic checks
    and collects definitions into a list of display types
    """

    synthetics: list[Type[SyntheticDisplayBase[Any]]] = []
    "The display type classes that have been defined"

    @override
    def __new__(
        cls: Type[_SyntheticMeta], name: str, bases: tuple[type, ...], namespace: dict[str, Any]
    ) -> Type[SyntheticDisplayBase[Any]]:
        new_class: Type[SyntheticDisplayBase[Any]] = type.__new__(cast(type, cls), name, bases, namespace)
        if namespace.get("__abstract__"):
            return new_class
        # Check for the required __typename__ and __parse__
        if not hasattr(new_class, "__typename__"):
            raise TypeError(f'Type "{new_class}" is missing a "__typename__" attribute')
        if not hasattr(new_class, "__parse__"):
            raise TypeError(f'Type "{new_class}" has no "__parse__" method')
        # Remember this new class:
        cls.synthetics.append(new_class)
        return new_class


class SyntheticDisplayBase(Generic[T], SBSyntheticValueProvider, metaclass=_SyntheticMeta):
    __abstract__: ClassVar[bool] = True
    "If true, disables metaclass checks"

    __summary_str__: ClassVar[str | None] = None
    "Set to an LLDB '--summary-string' formatting string for rendering the inline value summary"
    __enable_synthetic__: ClassVar[bool] = True
    "If False, do not generate synthetic children (used for primitive values)"

    if TYPE_CHECKING:
        __typename__: ClassVar[str]
        """
        The (regular expression) name of the type that is handled by this class.
        This is a required class attribute.
        """

        @classmethod
        def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
            """
            If provided, supplies the Python function that will dynamically
            generate a summary string from the SBValue.
            """
            ...

        @classmethod
        def __parse__(cls, value: SBValue) -> T:
            """
            Required method: Parse the data that lives at the given SBValue into
            a representation that will be useful for later display.
            """
            ...

    @classmethod
    @print_errors
    def __get_sbtype__(cls, frame: SBFrame, addr: int) -> SBType:
        """
        Obtain the SBType for this class. Can be overriden in subclasses, and
        the type may consider the value that lives at the address.
        """
        return generate_or_get_type(f"struct {cls.__typename__} {{}}", frame)

    @print_errors
    def __init__(self, val: SBValue, idict: InternalDict | None = None) -> None:
        self.__sbvalue = val
        "The SBValue given for this object"
        self.__children: list[ChildItem] = []
        "The synthetic children associated with the value"
        self.__value: T | None = None
        "The decoded value, or ``None`` if it has not yet been decoded"

    @property
    def sbvalue(self) -> SBValue:
        """Obtain the SBValue associated with this display object"""
        return self.__sbvalue

    @property
    def value(self) -> T:
        """Obtain the decoded BSON value"""
        if self.__value is None:
            self.__value = self.__parse__(self.sbvalue)
        return self.__value

    @property
    def address(self) -> int:
        """Obtain the memory address of the associated object"""
        return self.sbvalue.load_addr

    @print_errors
    @override
    def update(self) -> bool | None:
        """Update the parsed value and the child objects"""
        self.__value = None  # Clear the value so it will be re-read
        self.__children = list(self.get_children())

    def get_children(self) -> Iterable[ChildItem]:
        """
        Subclasses must provide this method such that it returns an iterable
        object of ``ChildItem`` types. These will be used to generate the
        synthetic children that are displayed by LLDB.
        """
        raise NotImplementedError

    @override
    def num_children(self) -> int:
        """Called by LLDB to know how many children exist"""
        n = len(self.__children)
        return n

    @override
    def has_children(self) -> bool:
        """Optimization opportunity for LLDB if it knows it doesn't need to ask"""
        return self.__enable_synthetic__ and self.num_children() != 0

    @print_errors
    @override
    def get_child_at_index(self, pos: int) -> SBValue:
        """
        Obtain the synthetic child of this value at index 'pos'.
        """
        # LLDB sometimes calls us with a child that we don't have?
        if pos >= len(self.__children):
            print(f"NOTE: lldb called get_child_at_index({pos}), but we only have {len(self.__children)} children")
            return SBValue()
        # Get the child:
        nth = self.__children[pos]
        # If not a tuple:
        if not isinstance(nth, tuple):
            # The type is a ValueFactory, which will return a new SBValue
            val = nth()
            assert val.error.success, f"{val.error=}, {nth=}, {pos=}"
            return val
        # Otherwise, they yielded a tuple:
        if len(nth) == 4:
            # A four-tuple of key, value, an LLDB value format, and an LLDB value type
            key, val, fmt, ty = nth
            sbval = self.create_value(key, val, format=fmt, cast_to=ty)
        else:
            # Just a key and a value
            key, val = nth
            sbval = self.create_value(key, val)
        assert sbval.error.success, (sbval.error, key, val)
        return sbval

    def create_value(
        self,
        name: str,
        value: str | int | float,
        cast_to: SBType | None = None,
        format: lldb.ValueFormatType | None = None,
    ) -> SBValue:
        """Cast a Python primitive into an LLDB SBValue."""
        # Encode the value in a JSON string, which will coincidentally be a valid
        # C expression that LLDB can parse:
        encoded = json.dumps(value)
        val = self.__sbvalue.CreateValueFromExpression(name, encoded)
        if cast_to:
            val = val.Cast(cast_to)
        if format is not None:
            val.format = format
        return val


class PrimitiveDisplay(Generic[T], SyntheticDisplayBase[T]):
    """
    Displays a primitive type. We can't use LLDB's basic types directly, since
    we must force a little-endian encoding.

    Set the __struct_format__ class variable to a Python struct format string.
    """

    __abstract__ = True
    __enable_synthetic__: ClassVar[bool] = False

    __struct_format__: ClassVar[str]
    "The struct format string that will be used to extract the value from memory"

    @classmethod
    @override
    @print_errors
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        return json.dumps(cls.__parse__(value))

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> T:
        unpack = struct.Struct(cls.__struct_format__)
        buf = memcache.get_cached(value.load_addr)[: unpack.size]
        val: T = unpack.unpack(buf)[0]
        return val


class DoubleDisplay(PrimitiveDisplay[float]):
    """Displays BSON doubles"""

    __typename__ = "__bson_double__"
    __struct_format__: ClassVar[str] = "<d"


class UTF8Display(SyntheticDisplayBase[bytes]):
    """Display type for BSON UTF-8 values"""

    __typename__ = "__bson_utf8__"
    __summary_str__ = "${var[1]}"  # Display the second child (which is the actual string)

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> bytes:
        buf = memcache.get_cached(value.load_addr)
        size = read_i32le(buf)
        return bytes(buf[4:][:size])

    @override
    def get_children(self) -> Iterable[ChildItem]:
        strlen = len(self.value)
        yield "size (bytes)", strlen
        # Create a char[] type to represent the string content:
        array_t = self.sbvalue.target.GetBasicType(lldb.eBasicTypeChar).GetArrayType(strlen)
        yield lambda: self.sbvalue.synthetic_child_from_address("[content]", self.address + 4, array_t)
        try:
            # Attempt a UTF-8 decode. We don't actually show this, we just want to
            # check if there are encoding errors, which we will display in the output
            self.value.decode("utf-8")
        except UnicodeDecodeError as e:
            yield "decode error", str(e)


class DocumentInfo(NamedTuple):
    """A decoded document"""

    elements: Sequence[DocumentElement | DocumentError]
    "Existing elements or errors found while parsing the data"


class DocumentElement(NamedTuple):
    """Represents an element within a document"""

    type: BSONType
    key: str
    value_offset: int
    "Offset from the beginning of the document data where the element's value appears"
    value_size: int
    "The size of the element's value (in bytes)"


class DocumentError(NamedTuple):
    """Represents an error while decoding a document"""

    message: str
    error_offset: int


class DocumentDisplay(SyntheticDisplayBase["DocumentInfo | DocumentError"]):
    """
    Main display of BSON document elements. This parses a document/array, and
    generates the child elements that can be further expanded and inspected.

    This type does not refer to the _MemCache, since this object is usually
    the top-level object and is the one responsible for filling the cache.
    """

    __typename__ = "__bson_document_[0-9]+__"
    __qualifier__: ClassVar[str] = "document"
    "The 'qualifier' of this type. Overriden by ArrayDisplay."

    @classmethod
    @override
    @print_errors
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        prefix = cls.__qualifier__
        doc = cls.__parse__(value)
        if isinstance(doc, DocumentError):
            return f"Error parsing {prefix} at byte {doc.error_offset}: {doc.message}"
        if len(doc.elements) == 0:
            return f"{prefix} (empty)"
        if len(doc.elements) == 1:
            return f"{prefix} (1 element)"
        return f"{prefix} ({len(doc.elements)} elements)"

    @classmethod
    @override
    def __get_sbtype__(cls, frame: SBFrame, addr: int) -> SBType:
        """Generate a unique type for the length of the document, allowing for byte-wise inspection"""
        # Read the size prefix:
        err = SBError()
        header = frame.thread.process.ReadMemory(addr, 4, err)
        assert err.success, f"{err=}, {frame=}, {addr=}"
        size = read_i32le(header)
        # Generate the type:
        typename = f"__bson_{cls.__qualifier__}_{size}__"
        doc_t = generate_or_get_type(
            f"""
            enum __bson_byte__ : unsigned char {{}};
            struct {typename} {{ __bson_byte__ bytes[{size}]; }}
            """,
            frame,
        )
        return doc_t

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> DocumentInfo | DocumentError:
        try:
            # Read from memory and refresh the segment in the memory cache. All
            # child element displays will re-use the same memory segment that
            # will be pulled here:
            buf = memcache.read(value)[1]
        except LookupError as e:
            return DocumentError(f"Failed to read memory: {e}", value.load_addr)
        return cls.parse_bytes(buf)

    @classmethod
    def parse_bytes(cls, buf: bytes) -> DocumentInfo | DocumentError:
        """Parse a document from the given data buffer"""
        elems = list(cls._parse_elems(buf))
        return DocumentInfo(elems)

    @classmethod
    def _parse_elems(cls, buf: bytes) -> Iterable[DocumentElement | DocumentError]:
        """Iteratively yield elements, or an error if parsing fails"""
        cur_offset = 4
        array_idx = 0
        while buf:
            elem = yield from cls._parse_one(buf[cur_offset:], cur_offset)
            if isinstance(elem, DocumentError):
                # An error ocurred, so we can't reliably continue parsing
                yield elem
                return
            if elem.type == BSONType.EOD:
                # This is the end.
                break
            # Yield this one, and then advance to the next element:
            yield elem
            elem_size = 1 + len(elem.key) + 1 + elem.value_size
            if cls.__qualifier__ == "array":
                # Validate that array keys are increasing integers:
                expect_key = str(array_idx)
                if elem.key != expect_key:
                    yield DocumentError(
                        f"Array element must have incrementing integer keys "
                        f'(Expected "{expect_key}", got "{elem.key}")',
                        cur_offset,
                    )
            array_idx += 1
            cur_offset += elem_size
        # Check that we actually consumed the whole buffer:
        remain = len(buf) - cur_offset
        if remain > 1:
            yield DocumentError(f"Extra {len(buf)} bytes in document data", cur_offset)

    @classmethod
    def _parse_one(
        cls, buf: bytes, elem_offset: int
    ) -> Generator[DocumentError, None, DocumentElement | DocumentError]:
        try:
            # Read the tag type
            type_tag = BSONType(buf[0])
        except ValueError:
            # The tag byte is not a valid tag value
            return DocumentError(f"Invalid element type tag 0x{buf[0]:x}", elem_offset)
        except IndexError:
            # 'buf' was empty
            return DocumentError(f"Unexpected end-of-data", elem_offset)
        # Stop if this is the end:
        if type_tag == BSONType.EOD:
            return DocumentElement(type_tag, "", 0, 0)
        # Find the null terminator on the key:
        try:
            key_nulpos = buf.index(0, 1)
        except ValueError:
            return DocumentError(f"Unexpected end-of-data while parsing the element key", elem_offset)
        key_bytes = buf[1:key_nulpos]
        try:
            key = key_bytes.decode("utf-8")
        except UnicodeDecodeError as e:
            yield DocumentError(f"Element key {key_bytes} is not valid UTF-8 ({e})", elem_offset)
            key = key_bytes.decode("utf-8", errors="replace")
        # The offset of the value within the element:
        inner_offset = key_nulpos + 1
        # The buffer that starts at the value:
        value_bytes = buf[inner_offset:]
        # Get the fixed size of the element:
        fixed_size = {
            BSONType.Double: 8,
            BSONType.Bool: 1,
            BSONType.Undefined: 0,
            BSONType.Null: 0,
            BSONType.MinKey: 0,
            BSONType.MaxKey: 0,
            BSONType.Datetime: 8,
            BSONType.ObjectID: 12,
            BSONType.Int32: 4,
            BSONType.Timestamp: 8,
            BSONType.Int64: 8,
            BSONType.Decimal128: 16,
        }
        value_size = fixed_size.get(BSONType(type_tag))
        if value_size is not None:
            pass  # This element has a fixed size
        elif type_tag in (BSONType.Code, BSONType.Symbol, BSONType.UTF8):
            # Size is 4 + a length prefix
            value_size = read_i32le(value_bytes) + 4
        elif type_tag in (BSONType.Array, BSONType.Document, BSONType.CodeWithScope):
            # Size is given by the length prefix
            value_size = read_i32le(value_bytes)
        elif type_tag == BSONType.DBPointer:
            # Size is a length prefix, plus four, plus 12 bytes for the OID
            value_size = read_i32le(value_bytes) + 4 + 12
        elif type_tag == BSONType.Regex:
            # Size is dynamic and given as two C strings:
            nul1 = value_bytes.index(0)
            nul2 = value_bytes.index(0, nul1 + 1)
            value_size = nul2 + 1
        elif type_tag == BSONType.Binary:
            # Size is a length prefix, plus four, plus one for the subtype
            value_size = read_i32le(value_bytes) + 4 + 1
        else:
            assert False, f"Unhandled value tag? {type_tag=} {buf=} {key=}"
        # The absolute offset of the element within the parent document:
        value_offset = elem_offset + inner_offset
        return DocumentElement(type_tag, key, value_offset, value_size)

    @override
    def get_children(self) -> Iterable[ChildItem]:
        doc = self.value
        if isinstance(doc, DocumentError):
            # The entire document failed to parse. Just generate one error:
            yield "[error]", f"Parsing error at byte {doc.error_offset}: {doc.message}"
            return
        for elem in doc.elements:
            if isinstance(elem, DocumentError):
                # There was an error at this location.
                yield "[error]", f"Data error at offset {elem.error_offset}: {elem.message}"
            else:
                # Create a ValueFactory for each element:
                yield functools.partial(self.create_child, self.sbvalue, elem)

    @classmethod
    def create_child(cls, parent: SBValue, elem: DocumentElement) -> SBValue:
        """Generate the child elements for LLDB to walk through"""
        if cls.__qualifier__ == "array":
            # Don't quote the integer keys
            name = f"[{elem.key}]"
        else:
            name = f"['{elem.key}']"
        value_addr = parent.load_addr + elem.value_offset
        frame = parent.frame
        # Create a new SBType to represent the element value. For each type tag, we
        # want a function that maps an SBFrame and an address to a new SBType:
        by_type: dict[BSONType, Callable[[SBFrame, int], SBType]] = {
            BSONType.Double: DoubleDisplay.__get_sbtype__,
            BSONType.UTF8: UTF8Display.__get_sbtype__,
            BSONType.Document: DocumentDisplay.__get_sbtype__,
            BSONType.Binary: BinaryDisplay.__get_sbtype__,
            BSONType.Array: ArrayDisplay.__get_sbtype__,
            # For bool, we can just use LLDB's basic type:
            BSONType.Bool: lambda _fr, _addr: parent.target.GetBasicType(lldb.eBasicTypeBool),
            BSONType.Code: CodeDisplay.__get_sbtype__,
            BSONType.CodeWithScope: CodeWithScopeDisplay.__get_sbtype__,
            BSONType.Int32: Int32Display.__get_sbtype__,
            BSONType.Int64: Int64Display.__get_sbtype__,
            BSONType.ObjectID: ObjectIDDisplay.__get_sbtype__,
            BSONType.DBPointer: DBPointerDisplay.__get_sbtype__,
            BSONType.Regex: RegexDisplay.__get_sbtype__,
            BSONType.Symbol: SymbolDisplay.__get_sbtype__,
            BSONType.Datetime: DatetimeDisplay.__get_sbtype__,
            BSONType.Timestamp: TimestampDisplay.__get_sbtype__,
            BSONType.Decimal128: Decimal128Display.__get_sbtype__,
            BSONType.Null: NullDisplay.__get_sbtype__,
            BSONType.Undefined: UndefinedDisplay.__get_sbtype__,
            BSONType.MaxKey: MaxKeyDisplay.__get_sbtype__,
            BSONType.MinKey: MinKeyDisplay.__get_sbtype__,
        }
        get_type = by_type.get(elem.type)
        assert get_type is not None, f"Unhandled type tag? {elem=}"
        # Create the SBType:
        type = get_type(frame, value_addr)
        # Create a synthetic child of that type at the address of the element's value:
        val = parent.synthetic_child_from_address(name, value_addr, type)
        assert val.error.success, f"{elem=}, {val.error=}"
        return val


class ArrayDisplay(DocumentDisplay):
    """Display for arrays. Most logic is implemented in the DocumentDisplay base."""

    __typename__ = "__bson_array_[0-9]+__"
    __qualifier__: ClassVar[str] = "array"


class BinaryInfo(NamedTuple):
    subtype: int
    data: bytes


class BinaryDisplay(SyntheticDisplayBase[BinaryInfo]):
    """Display for a BSON binary value"""

    __typename__ = "__bson_binary__"

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> BinaryInfo:
        buf = memcache.get_cached(value.load_addr)
        # Size prefix:
        data_size = read_i32le(buf)
        # Type tag:
        type = buf[4]
        # The remainder of the data:
        data = buf[5:][:data_size]
        return BinaryInfo(type, data)

    @override
    def get_children(self) -> Iterable[ChildItem]:
        yield "size", len(self.value.data)
        byte_t = generate_or_get_type("enum __bson_byte__ : char {}", self.sbvalue.frame)
        yield "subtype", self.value.subtype, lldb.eFormatHex, byte_t
        array_t = byte_t.GetArrayType(len(self.value.data))
        yield lambda: self.sbvalue.synthetic_child_from_address("data", self.address + 5, array_t)


class UndefinedDisplay(SyntheticDisplayBase[None]):
    """
    Display type for 'undefined' values. Also derived from for other unit types.
    """

    __typename__ = "__bson_undefined__"
    __summary_str__ = "undefined"
    __enable_synthetic__: ClassVar[bool] = False

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> None:
        return None


class ObjectIDDisplay(SyntheticDisplayBase[bytes]):
    """Display type for ObjectIDs"""

    __typename__ = "__bson_objectid__"

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        val = cls.__parse__(value)
        return f'ObjectID("{val.hex()}")'

    @classmethod
    @override
    def __get_sbtype__(cls, frame: SBFrame, addr: int) -> SBType:
        return generate_or_get_type(
            r"""
            enum __bson_byte__ : char {};
            struct __bson_objectid__ { __bson_byte__ bytes[12]; }
            """,
            frame,
        )

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> bytes:
        buf = memcache.get_cached(value.load_addr)
        return buf[:12]

    @override
    def get_children(self) -> Iterable[ChildItem]:
        yield "spelling", self.value.hex()


class DatetimeDisplay(SyntheticDisplayBase[int]):
    """Display for BSON Datetime objects"""

    __typename__ = "__bson_datetime__"
    __summary_str__: ClassVar[str] = "datetime: ${var[0]}"

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        dt = datetime.fromtimestamp(cls.__parse__(value) / 1000)
        s = f"{dt:%a %b %m %Y %H:%M:%S +%fμs}"
        return f'Date("{s}")'

    @classmethod
    @override
    def __parse__(cls, val: SBValue) -> int:
        buf = memcache.get_cached(val.load_addr)
        buf = buf[:8]
        value: int = struct.unpack("<Q", buf)[0]
        return value

    @override
    def get_children(self) -> Iterable[ChildItem]:
        # We can create a rich display using Python's datetime parsing:
        dt = datetime.fromtimestamp(self.value / 1000)
        # Adjusted to the local time zone:
        adjusted = dt.astimezone()
        yield from {
            "[isoformat]": dt.isoformat(),
            "[date]": f"{dt:%B %d, %Y}",
            "[time]": dt.strftime("%H:%M:%S +%fμs"),
            "[local]": adjusted.strftime("%c"),
            "Year": dt.year,
            "Month": dt.month,
            "Day": dt.day,
            "Hour": dt.hour,
            "Minute": dt.minute,
            "Second": dt.second,
            "+μs": dt.microsecond,
        }.items()


class NullDisplay(UndefinedDisplay):
    """Display for the BSON 'null' type"""

    __typename__ = "__bson_null__"
    __summary_str__ = "null"


class RegexDisplay(SyntheticDisplayBase[Tuple[bytes, bytes]]):
    """Display type for BSON regular expressions"""

    __typename__ = "__bson_regex_[0-9]+_[0-9]+__"
    __enable_synthetic__: ClassVar[bool] = False

    @classmethod
    @override
    def __get_sbtype__(cls, frame: SBFrame, addr: int) -> SBType:
        regex, opts = cls.parse_at(addr)
        regex_len = len(regex) + 1
        opts_len = len(opts) + 1
        # Synthesize a struct with two char[] matching the strings:
        return generate_or_get_type(
            rf"""
            struct __bson_regex_{regex_len}_{opts_len}__ {{
                char regex[{regex_len}];
                char options[{opts_len}];
            }}
            """,
            frame,
        )

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> tuple[bytes, bytes]:
        return cls.parse_at(value.load_addr)

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        # Create a JS-style regex literal:
        pair = cls.__parse__(value)
        regex, options = cls.decode_pair(pair)
        regex = regex.replace("/", "\\/").replace("\n", "\\n")
        return f"/{regex}/{options}"

    @classmethod
    def parse_at(cls, addr: int) -> tuple[bytes, bytes]:
        buf = memcache.get_cached(addr)
        # A regex is encoded with two C-strings. Find the nulls:
        nulpos_1 = buf.index(0)
        nulpos_2 = buf.index(0, nulpos_1 + 1)
        # Split them up:
        regex = buf[:nulpos_1]
        options = buf[nulpos_1 + 1 : nulpos_2]
        # Decode and return:
        return regex, options

    @classmethod
    def decode_pair(cls, value: tuple[bytes, bytes]) -> tuple[str, str]:
        regex, options = value
        regex = regex.decode("utf-8", errors="replace")
        options = options.decode("utf-8", errors="replace")
        return regex, options


class DBPointerDisplay(SyntheticDisplayBase[Tuple[bytes, int]]):
    """Display type for DBPointers"""

    __typename__ = "__bson_dbpointer__"
    __summary_str__: ClassVar[str | None] = "DBPointer(${var[0]}, ${var[1]})"

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> tuple[bytes, int]:
        buf = memcache.get_cached(value.load_addr)
        # Grab the string and the OID position
        strlen = read_i32le(buf)
        size = strlen + 4 + 12
        return buf[:size], strlen + 4

    @override
    def get_children(self) -> Iterable[ChildItem]:
        utf8_t = UTF8Display.__get_sbtype__(self.sbvalue.frame, self.address)
        oid_offset = self.value[1]
        oid_t = ObjectIDDisplay.__get_sbtype__(self.sbvalue.frame, self.address + oid_offset)
        yield lambda: self.sbvalue.synthetic_child_from_address("collection", self.sbvalue.load_addr, utf8_t)
        yield lambda: self.sbvalue.synthetic_child_from_address("object", self.sbvalue.load_addr + oid_offset, oid_t)


class CodeDisplay(UTF8Display):
    """Display type for BSON code"""

    __typename__ = "__bson_code__"
    __summary_str__ = "Code(${var[1]})"


class SymbolDisplay(UTF8Display):
    """Display type for BSON symbols"""

    __typename__ = "__bson_symbol__"

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        spell = cls.__parse__(value)
        dec = spell.decode("utf-8", errors="replace").rstrip("\x00")
        return f"Symbol({dec})"


class CodeWithScopeDisplay(SyntheticDisplayBase[int]):
    """Display type for BSON 'Code w/ Scope'"""

    __typename__ = "__code_with_scope__"

    __summary_str__: ClassVar[str | None] = "Code(${var[0][1]}, ${var[1]})"

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> int:
        buf = memcache.get_cached(value.load_addr)
        # Find the end position of the string:
        strlen = read_i32le(buf[4:])
        str_end = 4 + strlen + 4
        return str_end

    @override
    def get_children(self) -> Iterable[ChildItem]:
        code_t = CodeDisplay.__get_sbtype__(self.sbvalue.frame, self.address)
        scope_doc_offset = self.value
        doc_t = DocumentDisplay.__get_sbtype__(self.sbvalue.frame, self.address + scope_doc_offset)
        yield lambda: checked(self.sbvalue.synthetic_child_from_address("code", self.address + 4, code_t))
        yield lambda: checked(
            self.sbvalue.synthetic_child_from_address("scope", self.address + scope_doc_offset, doc_t)
        )


class Int32Display(PrimitiveDisplay[int]):
    """Display for 32-bit BSON integers"""

    __typename__ = "__bson_int32__"
    __struct_format__: ClassVar[str] = "<i"

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        return f"NumberInt({cls.__parse__(value)})"


class Int64Display(PrimitiveDisplay[int]):
    """Display for 64-bit BSON integers"""

    __typename__ = "__bson_int64__"
    __struct_format__: ClassVar[str] = "<q"

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        return f"NumberLong({cls.__parse__(value)})"


class TimestampDisplay(SyntheticDisplayBase[Tuple[int, int]]):
    """Display type for BSON timestamps"""

    __typename__ = "__bson_timestamp__"
    __summary_str__ = "Timestamp(${var[0]}, ${var[1]})"

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> tuple[int, int]:
        buf = memcache.get_cached(value.load_addr)[:8]
        # Just two 32bit integers:
        timestamp, increment = struct.unpack("<ii", buf)
        return timestamp, increment

    @override
    def get_children(self) -> Iterable[ChildItem]:
        yield "timestamp", self.value[0]
        yield "increment", self.value[1]


class Decimal128Value(NamedTuple):
    """Represents a parsed Decimal128 value"""

    sign: int
    combination: int
    exponent: int
    significand: int
    spelling: str


class Decimal128Display(SyntheticDisplayBase[Decimal128Value]):
    """The display type for BSON's Decimal128 type"""

    __typename__ = "__bson_decimal128__"

    @classmethod
    @override
    def __get_sbtype__(cls, frame: SBFrame, addr: int) -> SBType:
        """Generate a type for byte-wise introspection"""
        return generate_or_get_type(
            r"""
            struct __bson_decimal128__ {
                unsigned char bytes[16];
            }
            """,
            frame,
        )

    @classmethod
    @override
    def __summary__(cls, value: SBValue, idict: InternalDict) -> str:
        val = cls.__parse__(value)
        return f'NumberDecimal("{val.spelling}")'

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> Decimal128Value:
        dat = bytes(value.data.uint8)
        # The value is little-endian encoded, so we manually read it in that encoding
        d1 = read_i32le(dat)
        d2 = read_i32le(dat[4:])
        d3 = read_i32le(dat[8:])
        d4 = read_i32le(dat[12:])
        # Combin the parts:
        low_word = (d2 << 32) | d1
        hi_word = (d4 << 32) | d3
        d128_tetra = (hi_word << 64) | low_word
        # Create an array of individual bits (high bits first):
        bits = tuple(((d128_tetra >> n) & 1) for n in range(127, -1, -1))
        # Recombine a sequence of bits into an int (high bits first)
        mergebits: Callable[[tuple[int, ...]], int] = lambda bs: functools.reduce(lambda a, b: (a << 1) | b, bs, 0)
        # Sign bit:
        sign = bits[0]
        # BID uses the first two combo bits to indicate that the exponent is shifted
        if bits[1] and bits[2]:
            shifted = True
        else:
            shifted = False

        spelling = None
        if not shifted:
            # Regular bit positions:
            exponent = bits[1:15]
            coeff = bits[15:]
        else:
            # Bit positions are shifted over
            exponent = bits[3:17]
            # The significand has an implicit '0b100' prepended as the highest bits:
            coeff = (1, 0, 0) + bits[17:]
            # Check for special values in the remainder of the combination:
            more = bits[3:6]
            if more == (1, 0, 0) or more == (1, 0, 1):
                spelling = "Infinity"
            elif more == (1, 1, 0):
                spelling = "NaN (quiet)"
            elif more == (1, 1, 1):
                spelling = "NaN (signaling)"

        coeff = mergebits(coeff)
        exponent = mergebits(exponent)
        if spelling is None:
            spelling = str(coeff)
            e = exponent - 6176
            if e == 0:
                pass
            elif e < 0:
                spelling = spelling.zfill(abs(e))
                split = len(spelling) + e
                w, fr = spelling[:split], spelling[split:]
                spelling = f"{w}.{fr}"
            else:
                spelling = spelling + "0" * e

        if sign:
            spelling = f"-{spelling}"

        # The "combination" bits
        combination = mergebits(bits[1:18])
        return Decimal128Value(sign, combination, exponent, coeff, spelling)

    @override
    def get_children(self) -> Iterable[ChildItem]:
        yield "sign", self.value.sign
        yield "combination", self.value.combination, lldb.eFormatBinary, None
        yield "exponent (biased)", self.value.exponent
        yield "exponent (actual)", self.value.exponent - 6176
        yield "significand", str(self.value.significand)
        yield "value", self.value.spelling


class MaxKeyDisplay(NullDisplay):
    """The display type for BSON's 'max key' type"""

    __typename__ = "__bson_maxkey__"
    __summary_str__ = "max key"


class MinKeyDisplay(NullDisplay):
    """The display type for BSON's 'min key' type"""

    __typename__ = "__bson_minkey__"
    __summary_str__ = "min key"


class BSONTInfo(NamedTuple):
    """Information about a bson_t object"""

    addr: int
    "The address of the pointer to the beginning of the BSON data managed by this object"
    size: int
    "The size of the BSON data managed/referenced by this object"
    flags: int
    "Flags of the bson_t object"


class BSONTError(NamedTuple):
    """Represents an error while reading a bson_t object"""

    reason: str
    "A description of the error that ocurred"


class BSONTDisplay(SyntheticDisplayBase["BSONTInfo | BSONTError"]):
    """
    Implements inspection logic for bson_t
    """

    __typename__ = "bson_t"

    @classmethod
    @override
    def __parse__(cls, value: SBValue) -> BSONTInfo | BSONTError:
        # Dereference pointers:
        if value.TypeIsPointerType():
            value = value.deref

        # Unwrap the wrapped object
        value = value.GetNonSyntheticValue()

        dat = value.data
        # We know the location of the flags and size without debug info:
        err = SBError()
        flags = dat.GetUnsignedInt32(err, 0)
        if err.fail:
            return BSONTError(f"Failed to read memory at 0x{value.load_addr:x}: {err.description}")
        length = dat.GetUnsignedInt32(err, 4)
        if err.fail:
            return BSONTError(f"Failed to read memory at 0x{value.load_addr:x}: {err.description}")

        # Check bogus values:
        MAX_SIZE = 16 * 1024 * 1024
        ALL_FLAGS = (1 << 6) - 1
        if flags & ~ALL_FLAGS or length < 5 or length > MAX_SIZE:
            return BSONTError(f"bson_t appears uninitialized/invalid [a] {flags=} {length=}")

        is_inline = bool(flags & 1)

        if is_inline:
            # Inline objects may only occupy 120 bytes, at most
            if length > 120:
                return BSONTError("bson_t appears uninitialized/invalid [b]")
            # Look for debug info for the inline impl
            inline_t = value.target.FindFirstType("bson_impl_inline_t")
            if inline_t:
                as_inline = value.Cast(inline_t)
                ptr = as_inline.GetChildMemberWithName("data").load_addr
            else:
                # No debug info? Guess its location as the default
                ptr = value.load_addr + 4 + 4
            if not err.success:
                return BSONTError(f"Failed to read inline bson_t data: {err}")
            return BSONTInfo(ptr, length, flags)

        # Look for impl_alloc_t
        alloc_t = value.target.FindFirstType("bson_impl_alloc_t")
        if alloc_t:
            alloc = value.Cast(alloc_t)
            # Walk to the buffer for this value:
            offset = alloc.GetChildMemberWithName("offset").unsigned
            buf = alloc.GetChildMemberWithName("buf").deref.deref
            ptr = buf.load_addr + offset
            return BSONTInfo(ptr, length, flags)

        # No debug info, we have to calc the offset ourself
        u8_t = value.target.GetBasicType(lldb.eBasicTypeUnsignedChar)
        u8ptr_t = u8_t.GetPointerType()
        ptr_size = u8ptr_t.size
        u32_size = 4
        buf_off = (u32_size * 2) + ptr_size + u32_size
        if u32_size < ptr_size:
            # Adjust for alignment
            buf_off += u32_size
        offset_off = buf_off + (ptr_size * 2)
        offset = dat.GetUnsignedInt32(err, offset_off)
        if not err.success:
            return BSONTError(f"Failed to read offset of buffer: {err}")
        bufptr = value.CreateChildAtOffset("buf", buf_off, u8ptr_t.GetPointerType()).deref
        if not bufptr.error.success:
            return BSONTError(f"Failed to read the alloc buf: {bufptr.error} {offset=} {buf_off=}")
        ptr = bufptr.data.GetUnsignedInt64(err, 0)
        assert err.success, err

        u32_t = value.target.FindFirstType("uint32_t")
        addr = SBAddress()
        addr.SetLoadAddress(ptr, value.target)

        u32 = value.target.CreateValueFromAddress("tmp", addr, u32_t)
        assert u32.error.success, u32
        if u32.unsigned != length or length < 5:
            return BSONTError(f"bson_t appears uninitialized/invalid [c] {flags=} {length=} {u32.unsigned=}")
        return BSONTInfo(ptr, length, flags)

    @override
    def get_children(self) -> Iterable[ChildItem]:
        val = self.value
        if isinstance(val, BSONTError):
            yield "[error]", val.reason
            return

        # Imbue the flags with the possible debug info to give it a nice rendering
        flags_t = self.sbvalue.target.FindFirstType("bson_flags_t")
        if flags_t.IsValid():
            yield "flags", val.flags, None, flags_t
        else:
            yield "flags", val.flags
        yield "data size", val.size
        ptr_t = self.sbvalue.target.GetBasicType(lldb.eBasicTypeVoid).GetPointerType()
        yield "data address", val.addr, lldb.eFormatPointer, ptr_t

        # Generate the __bson_document_xxx__ that will allow walking the document:
        doc_t = DocumentDisplay.__get_sbtype__(self.sbvalue.frame, val.addr)
        yield lambda: checked(self.sbvalue.synthetic_child_from_address("[content]", val.addr, doc_t))


def checked(val: SBValue) -> SBValue:
    """Assert that ``val`` is valid. Returns ``val``"""
    assert val.error.success, f"{val=} {val.error=}"
    return val


def read_i32le(dat: bytes) -> int:
    """Read a 32-bit integer from the given data."""
    # Truncate before the read:
    buf = dat[0:4]
    return struct.unpack("<i", buf)[0]


_types_cache: dict[tuple[int, str], SBType] = {}
"The cache of generated types (for generate_or_get_type)"


def generate_or_get_type(expr_prefix: str, frame: SBFrame) -> SBType:
    """
    This is the big magic in this module: LLDB doesn't have a "simple" API to
    generate new datatypes (which we need in order to inspect element contents!),
    so we instead generate them on-the-fly using the expression evaluator.

    If we evaluate an expression like:

        struct some_struct { int a, b, c; };
        some_struct x;
        x

    LLDB will give us a handle to ``x``. We can then ask for the type of ``x``,
    which gives us a handle to `some_struct`, from which we can now synthesize
    new values and use as if it were part of the target's debug information all
    along.

    This needs to be done carefully, since it is an expensive operation and
    we do not want to declare the same type more than once. For this reason,
    we cache.
    """
    # The cache key
    cachekey = frame.thread.process.id, expr_prefix
    existing = _types_cache.get(cachekey)
    if existing is not None:
        # We've already generated this type before
        return existing
    # Create a new temporary object. Give it a unique name to prevent it from
    # colliding with any possible temporaries we may have generated previously.
    hash = hashlib.md5(expr_prefix.encode()).hexdigest()
    varname = f"__bson_lldb_tmp_{hash}"
    full_expr = f"{expr_prefix} {varname}; {varname}"
    tmp = frame.EvaluateExpression(full_expr)
    existing = tmp.type
    _types_cache[cachekey] = existing
    return existing


class BSONType(enum.Enum):
    """The values for each bson element type tag"""

    EOD = 0
    Double = 0x1
    UTF8 = 0x2
    Document = 0x3
    Array = 0x4
    Binary = 0x5
    Undefined = 0x6
    ObjectID = 0x7
    Bool = 0x8
    Datetime = 0x9
    Null = 0xA
    Regex = 0xB
    DBPointer = 0xC
    Code = 0xD
    Symbol = 0xE
    CodeWithScope = 0xF
    Int32 = 0x10
    Timestamp = 0x11
    Int64 = 0x12
    Decimal128 = 0x13
    MinKey = 255
    MaxKey = 127


class _BSONWalker:
    """
    This implement document traversal in a Python expression evaluator. It
    uses __getattr__ and __getitem__ to provide simple semantics for accessing
    subelements, and uses the matmul infix operator ``@`` to resolve the path
    against a given SBValue that refers to a ``bson_t``
    """

    def __init__(self, path: Iterable[str | int] = ()) -> None:
        self._path = tuple(path)
        "The path that we will resolve when '@' is applied."

    def __rmatmul__(self, lhs: Any) -> SBValue | str:
        """Implement the actual path resolution. This is the '@' operator."""
        if isinstance(lhs, str):
            # Evaluate the left-hand string as an expression within the target
            target = lldb.debugger.GetSelectedTarget()
            if target is None:
                raise RuntimeError("Not attached to a debug target")
            frame = target.process.selected_thread.frames[0]
            lhs = frame.EvaluateExpression(lhs)
        val: SBValue
        if hasattr(lhs.__class__, "unwrap"):
            # CodeLLDB gives us a wrapper around SBValue, but we want the unwrapped
            # version:
            val = lhs.__class__.unwrap(lhs)
        else:
            val = lhs

        if val.TypeIsPointerType():
            # Dereference pointers
            val = val.deref

        # Drop the synthetic wrapper that was wrapped around bson_t
        val = val.GetNonSyntheticValue()

        # Parse it:
        as_bson = BSONTDisplay.__parse__(val)
        if isinstance(as_bson, BSONTError):
            raise ValueError(as_bson.reason)

        # Create the synthetic __bson_document_xxx__ object for this doc
        doc_t = DocumentDisplay.__get_sbtype__(val.frame, as_bson.addr)
        # Obtain a value reference to the document data:
        retval = val.CreateValueFromAddress("[root]", as_bson.addr, doc_t)

        # Now resolve the path:
        for part in self._path:
            if isinstance(part, str):
                # Access via ``p['foo']`` or ``p.foo``, requires our current node
                # to be a document:
                if not retval.type.name.startswith("__bson_document_"):
                    raise AttributeError(
                        f'Element of type {retval.type.name} cannot be accessed as a document (looking for element "{part}")'
                    )
                # We are accessing the synthetic children generated by DocumentDisplay.
                # The keys are of the following format based on the element keys in the document:
                want_child_name = f"['{part}']"
            else:
                # Access via indexing ``p[42]``, requires an array
                if not retval.type.name.startswith("__bson_array_"):
                    raise AttributeError(
                        f"Element of type {retval.type.name} cannot be accessed as an array (looking for element {part})"
                    )
                # Array keys are bracketed, but not quoted
                want_child_name = f"[{part}]"
            # Find all children that match the key (usually only one)
            matching = (c for c in retval.children if c.name == want_child_name)
            # Get it:
            got = next(iter(matching), None)
            if got is None:
                # Didn't get it...
                if isinstance(part, str):
                    raise KeyError(f'Document has no element "{part}"')
                else:
                    raise IndexError(f"Array index [{part}] is out-of-bounds")
            # Set this as our current node, which we may step in further, or
            # we may be done
            retval = got

        return retval

    def __getattr__(self, key: str) -> _BSONWalker:
        """Generate a new traversal for the given key"""
        return _BSONWalker(self._path + (key,))

    def __getitem__(self, key: str | int) -> _BSONWalker:
        """Generate a new traversal for the given key or index"""
        return _BSONWalker(self._path + (key,))


class _MemoryCache:
    """
    Reading process memory is an extremely slow operation, and we don't want to
    re-read memory multiple times. Because parsing a document's elements requires
    reading the entire document into memory, we are guaranteed that the top-level
    DocumentDisplay object will perform a ReadMemory() of the entire object, and
    thus all child elements can reuse that buffer when they parse and render
    their own contents.

    This class implements that cache of memory segments
    """

    def __init__(self):
        self._segments: dict[int, bytes] = {}
        "Segments of memory keyed by the base address of the read operation"

    def get_cached(self, addr: int) -> bytes:
        """
        Find and return a chunk of memory that was previously read from the
        process. The ``addr`` must be an address within a BSON document that
        was previously read by a DocumentDisplay.

        The returned buffer begins at exactly ``addr``, and contains the full
        remainder of the cached segment, which may be much longer than the
        caller actually needs. We don't need additional granularity for our
        purposes, though.
        """
        segment = self.segment_containing(addr)
        if not segment:
            # Memory does not exist?
            print(f"lldb_bson: Note: Attempted read of uncached address 0x{addr:x}")
            print("".join(traceback.format_stack()))
            return b"\0" * 512
        base_addr, data = segment
        inner_offset = addr - base_addr
        return data[inner_offset:]

    def segment_containing(self, addr: int) -> tuple[int, bytes] | None:
        """Find the segment that contains ``addr``, or returns None"""
        for base_addr, data in self._segments.items():
            end_addr = base_addr + len(data)
            if addr > end_addr or addr < base_addr:
                continue
            return base_addr, data
        return None

    def read(self, val: SBValue) -> tuple[int, bytes]:
        """
        Read the memory segment referenced by the given SBValue
        """
        return self.read_at(val.process, val.load_addr, val.size)

    def read_at(self, proc: SBProcess, addr: int, size: int) -> tuple[int, bytes]:
        """
        Read from ``proc`` a chunk of memory beginning at ``addr`` and continuing
        for ``size`` bytes.
        """
        err = SBError()
        buf = proc.ReadMemory(addr, size, err)
        if err.fail:
            raise LookupError(err.description)
        self._segments[addr] = buf
        return addr, buf


memcache = _MemoryCache()
"A module-wide memory segment cache."
