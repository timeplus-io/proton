#include <CPython/Utils.h>
#include <Poco/JSON/Array.h>
#include <Poco/UUIDGenerator.h>
#include <Common/Exception.h>

#include <sstream>
#include <vector>

namespace DB::ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_RUNNING_ERROR;
extern const int QUERY_WAS_CANCELLED;
}

namespace DB::cpython
{
namespace
{
std::string uuidStringWithUnderscore()
{
    static Poco::UUIDGenerator generator;
    auto id = generator.createRandom().toString();
    std::replace(id.begin(), id.end(), '-', '_');
    return id;
}
}

std::string randomModuleName()
{
    return "module_" + uuidStringWithUnderscore();
}

std::string convertPyObjectToString(const PyObjectPtr & obj)
{
    PyObjectPtr obj_py_str{PyObject_Str(obj.get())};
    if (hasException())
    {
        std::string error_message = getExceptionMessage();
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Convert Python varible to string error, detail message : {}", error_message);
    }

    return std::string{PyUnicode_AsUTF8(obj_py_str.get())};
}

bool hasException()
{
    return PyErr_Occurred() != nullptr;
}

/// This function only accesses the attributes of the object itself and does not involve changes to the reference count.
/// Using `PyObjectPtr` to wrap it would make the code particularly bloated.
/// Therefore, we keep raw pointers here for now.
std::string convertTracebackToString(const PyTracebackObject * tb)
{
    if (!tb)
        return {};

    std::stringstream result;

    std::string filename;

    if (auto * tb_frame = tb->tb_frame; tb_frame)
    {
        /// PyFrame_GetCode returns a strong reference (available since 3.9).
        /// PyFrameObject internals are opaque from 3.11+, so direct f_code
        /// access is not portable.
        PyCodeObject * code = PyFrame_GetCode(tb_frame);
        if (code)
        {
            /// In CPython 3.12+, tb_lineno may be -1 (lazy).  Resolve via
            /// PyCode_Addr2Line when that happens.
            int tb_line = tb->tb_lineno;
            if (tb_line == -1)
                tb_line = PyCode_Addr2Line(code, tb->tb_lasti);

            filename = PyUnicode_AsUTF8(code->co_filename);
            result << fmt::format("File \"{}\", line {}, in {}\n", filename, tb_line, PyUnicode_AsUTF8(code->co_name));
            Py_DECREF(code);
        }
    }

    result << convertTracebackToString(tb->tb_next);

    return result.str();
}

std::string getExceptionMessage()
{
    chassert(hasException());

    PyObject * rptype;
    PyObject * rpvalue;
    PyObject * rptraceback;

    PyErr_Fetch(&rptype, &rpvalue, &rptraceback);
    PyErr_NormalizeException(&rptype, &rpvalue, &rptraceback);

    PyObjectPtr ptype(rptype);
    PyObjectPtr pvalue(rpvalue);
    PyObjectPtr ptraceback(rptraceback);

    /// Free-threaded CPython (PEP 703) splits ob_refcnt into ob_ref_local /
    /// ob_ref_shared, so we go through the Py_REFCNT() accessor which masks
    /// the biased-refcount layout from callers.
    if (ptype && Py_REFCNT(ptype.get()) == 1 && !PyObject_IS_GC(ptype.get()))
        ptype.release();

    PyErr_Clear();

    try
    {
        std::stringstream ss;
        ss << convertPyObjectToString(pvalue) << ": " << std::endl;

        if (ptraceback && PyTraceBack_Check(ptraceback.get()))
            ss << convertTracebackToString(reinterpret_cast<PyTracebackObject *>(ptraceback.get())) << std::endl;

        return ss.str();
    }
    catch (const Exception & e)
    {
        return e.displayText();
    }
}

PyObjectPtr getAttr(const PyObjectPtr & obj, const std::string & attr)
{
    PyObjectPtr res{PyObject_GetAttrString(obj.get(), attr.c_str())};

    if (hasException())
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to get attribute: {}", getExceptionMessage());

    return res;
}

PyObjectPtr tryGetAttr(const PyObjectPtr & obj, const std::string & attr)
{
    if (!obj)
        return PyObjectPtr{};

    /// PyObject_GetOptionalAttrString (3.13+) is the proper replacement for
    /// PyObject_HasAttrString + PyObject_GetAttrString.  It returns 1 if
    /// found, 0 if not found, -1 on error — without silently swallowing
    /// exceptions from __getattribute__.
    PyObject * result = nullptr;
    int rc = PyObject_GetOptionalAttrString(obj.get(), attr.c_str(), &result);
    if (rc < 0)
    {
        PyErr_Clear();
        return PyObjectPtr{};
    }
    if (rc == 0)
        return PyObjectPtr{};

    return PyObjectPtr{result};
}

PyObjectPtr importModule(const std::string & module_name)
{
    PyObjectPtr res{PyImport_ImportModule(module_name.c_str())};
    if (hasException())
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to import module {}: {}", module_name, getExceptionMessage());
    return res;
}

PyObjectPtr getOrAddModule(const std::string & module_name)
{
    /// PyImport_AddModule returns a borrowed reference which will not increase the reference count
    /// If we return PyObjectPtr, we should add reference count to avoid the object being released
    auto res = PyObjectPtr::borrow(PyImport_AddModule(module_name.c_str()));

    return res;
}

PyObjectPtr getOrAddMainModule(const std::string & module_name)
{
    /// PyImport_AddModule returns a borrowed reference which will not increase the reference count
    /// If we return PyObjectPtr, we should add reference count to avoid the object being released
    auto res = PyObjectPtr::borrow(PyImport_AddModule(module_name.c_str()));

    /// The below code is copied from CPython
    /// It will prepare the attribute which is needed by the main module
    if (!res)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to add module: {}", module_name);

    auto d = PyObjectPtr::borrow(PyModule_GetDict(res.get()));
    PyObjectPtr ann_dict{PyDict_New()};
    if (!ann_dict || (PyDict_SetItemString(d.get(), "__annotations__", ann_dict.get()) < 0))
    {
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to initialize {}.__annotations__", module_name);
    }

    PyObjectPtr builtins_key{PyUnicode_FromString("__builtins__")};
    if (!builtins_key)
    {
        /// PyDict_Contains hashes the key; a NULL key (allocation failure) would
        /// dereference NULL in PyObject_Hash rather than return -1.
        PyErr_Clear();
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate {}.__builtins__ key", module_name);
    }

    int has_builtins = PyDict_Contains(d.get(), builtins_key.get());
    if (has_builtins < 0)
    {
        PyErr_Clear();
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to test {}.__builtins__", module_name);
    }
    if (!has_builtins)
    {
        PyObjectPtr bimod{PyImport_ImportModule("builtins")};
        if (!bimod)
        {
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to retrieve builtins module");
        }
        if (PyDict_SetItemString(d.get(), "__builtins__", bimod.get()) < 0)
        {
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to initialize {}.__builtins__", module_name);
        }
    }

    return res;
}

namespace
{
/// Public-API re-implementation of CPython's internal _PyModule_Clear.
/// PyDict_Clear removes every key, so any __del__ that fires during the
/// subsequent decref chain raises NameError on module globals it touches
/// (including print / len — __builtins__ is gone). _PyModule_Clear keeps
/// the keys and replaces their values with None, preserving __builtins__
/// verbatim so destructor code can still use it. Two-pass order matches
/// CPython: single-underscore privates first (destructor ordering hint),
/// then everything else except __builtins__.
///
/// Semantics are from contrib/cpython/Objects/moduleobject.c:731
/// (_PyModule_ClearDict). Reproduced via public API only; no pycore_* deps.
///
/// Implementation note: CPython's internal version mutates values during
/// PyDict_Next. That is safe by contract but relies on the dict's internal
/// version counter behaviour, which differs under PEP 703 free-threaded
/// iteration — PyDict_Next has been observed to silently drop not-yet-visited
/// entries after an in-place value replacement on 3.14t. We snapshot the keys
/// first, holding an OWNED ref to each (the SetItem(None) pass decrefs old
/// values, which can run __del__ that mutates/clears this dict and would free
/// a borrowed key before we visit it), and then do the substitution, which is
/// deterministic on both builds.
void clearModuleDictLikeCPython(PyObject * d)
{
    if (!d)
        return;

    std::vector<PyObjectPtr> pass1_keys; /// single-underscore names (_foo)
    std::vector<PyObjectPtr> pass2_keys; /// everything else except __builtins__

    Py_ssize_t pos = 0;
    PyObject * key = nullptr;
    PyObject * value = nullptr;

    while (PyDict_Next(d, &pos, &key, &value))
    {
        if (value == Py_None || !PyUnicode_Check(key))
            continue;
        if (PyUnicode_CompareWithASCIIString(key, "__builtins__") == 0)
            continue;

        /// borrow() increfs: keep an owned ref so a __del__ fired by the
        /// SetItem(None) pass below cannot free a still-queued key.
        if (PyUnicode_GetLength(key) >= 2
            && PyUnicode_READ_CHAR(key, 0) == '_'
            && PyUnicode_READ_CHAR(key, 1) != '_')
            pass1_keys.push_back(PyObjectPtr::borrow(key));
        else
            pass2_keys.push_back(PyObjectPtr::borrow(key));
    }

    for (const auto & k : pass1_keys)
    {
        if (PyDict_SetItem(d, k.get(), Py_None) != 0)
            PyErr_Clear();
    }
    for (const auto & k : pass2_keys)
    {
        if (PyDict_SetItem(d, k.get(), Py_None) != 0)
            PyErr_Clear();
    }
}
}

void unloadModule(const std::string & module_name)
{
    /// PyImport_GetModuleDict only get the pointer from interpreter
    /// and not increase its reference count.
    /// We shouldn't wrap it with PyObjectPtr
    PyObjectPtr sys_module{PyImport_GetModuleDict()};

    if (auto py_module = getModule(module_name))
        clearModuleDictLikeCPython(PyModule_GetDict(py_module.get()));

    if (PyDict_DelItemString(sys_module.release(), module_name.c_str()) != 0)
    {
        PyErr_Clear();
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to unload module: {}", module_name);
    }
}

PyObjectPtr getModule(const std::string & module_name)
{
    PyObjectPtr name{PyUnicode_FromString(module_name.c_str())};
    if (!name)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to create module name object: {}", getExceptionMessage());

    PyObjectPtr module{PyImport_GetModule(name.get())};
    return module;
}

bool hasModule(const std::string & module_name)
{
    /// PyImport_GetModuleDict only get the pointer from interpreter
    /// and not increase its reference count.
    /// We shouldn't wrap it with PyObjectPtr
    auto sys_module = PyImport_GetModuleDict();
    if (!sys_module)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to get sys module: {}", getExceptionMessage());

    PyObjectPtr module_name_obj{PyUnicode_FromString(module_name.c_str())};
    return PyDict_Contains(sys_module, module_name_obj.get());
}

std::string getObjectType(const PyObjectPtr & obj)
{
    PyObjectPtr type{PyObject_Type(obj.get())};
    return convertPyObjectToString(type);
}

PyObjectPtr getFunction(const std::string & func_name, const std::string & module_name)
{
    auto main_module = getOrAddMainModule(module_name);
    auto py_func = tryGetAttr(main_module, func_name);

    if (!py_func)
        throw Exception(
            ErrorCodes::UDF_INTERNAL_ERROR, "The python function name is not the same as the user defined function name: {}", func_name);
    return py_func;
}

PyObjectPtr getClass(const std::string & class_name, const std::string & module_name)
{
    auto main_module = getOrAddMainModule(module_name);
    auto py_class = tryGetAttr(main_module, class_name);

    if (!py_class)
        throw Exception(
            ErrorCodes::UDF_INTERNAL_ERROR, "The python class name is not the same as the user defined function name: {}", class_name);
    return py_class;
}

void unloadClass(const PyObjectPtr & py_class)
{
    if (py_class && Py_TYPE(py_class.get())->tp_clear)
        Py_TYPE(py_class.get())->tp_clear(py_class.get());
}

PyObjectPtr newInstance(const PyObjectPtr & py_class, const PyObjectPtr & args)
{
    return executeObject(py_class, args);
}

PyObjectPtr getInstanceMethod(const PyObjectPtr & py_instance, const std::string & method_name)
{
    auto py_method = tryGetAttr(py_instance, method_name);
    if (!py_method)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "The python class does not have the method: {}", method_name);
    return py_method;
}

PyObjectPtr compile(const std::string & source)
{
    /// compile the python code , throw exception if the code is invalid
    PyObjectPtr byte_code{Py_CompileString(source.c_str(), "", Py_file_input)};
    if (!byte_code)
    {
        std::string error_message = getExceptionMessage();
        throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the Python UDF is invalid. Detail error: {}", error_message);
    }

    return byte_code;
}

PyObjectPtr executeByteCode(const PyObjectPtr & byte_code, const std::string & module_name)
{
    /// FIXME: when eval byte code, we'd better to pass the global and local dict
    /// it will be helpful to isolate different execution env.
    auto main_module = getOrAddMainModule(module_name);
    PyObjectPtr exe_result{PyEval_EvalCode(byte_code.get(), PyModule_GetDict(main_module.get()), nullptr)};

    if (!exe_result)
    {
        if (hasException() && (PyErr_ExceptionMatches(PyExc_KeyboardInterrupt) || PyErr_ExceptionMatches(PyExc_GeneratorExit)))
        {
            PyErr_Clear();
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        }
        std::string error_message = getExceptionMessage();
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "UDF running error, detail message: {}", error_message);
    }

    return exe_result;
}

PyObjectPtr executeObject(const PyObjectPtr & obj, const PyObjectPtr & args)
{
    PyObjectPtr exe_result{PyObject_CallObject(obj.get(), args.get())};

    if (!exe_result)
    {
        if (hasException() && (PyErr_ExceptionMatches(PyExc_KeyboardInterrupt) || PyErr_ExceptionMatches(PyExc_GeneratorExit)))
        {
            PyErr_Clear();
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
        }
        std::string error_message = getExceptionMessage();
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "UDF running error, detail message: {}", error_message);
    }

    return exe_result;
}

PyObjectPtr createArgumentsTuple(const Strings & arguments)
{
    PyObjectPtr py_args{PyTuple_New(static_cast<Py_ssize_t>(arguments.size()))};
    if (!py_args)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python arguments tuple: {}", getExceptionMessage());

    for (size_t i = 0; i < arguments.size(); ++i)
    {
        PyObjectPtr py_arg{PyUnicode_FromStringAndSize(arguments[i].data(), static_cast<Py_ssize_t>(arguments[i].size()))};
        if (!py_arg)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert Python argument to string: {}", getExceptionMessage());

        if (PyTuple_SetItem(py_args.get(), static_cast<Py_ssize_t>(i), py_arg.release()) != 0)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to set Python argument tuple item: {}", getExceptionMessage());
    }

    return py_args;
}

PyObjectPtr convertJSONValueToPyObject(const Poco::Dynamic::Var & value)
{
    if (value.isEmpty())
        return PyObjectPtr::borrow(Py_None);

    if (value.type() == typeid(Poco::JSON::Object::Ptr))
    {
        auto json_object = value.extract<Poco::JSON::Object::Ptr>();
        PyObjectPtr py_dict{PyDict_New()};
        if (!py_dict)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python dict for JSON object: {}", getExceptionMessage());

        for (const auto & key : json_object->getNames())
        {
            PyObjectPtr py_value = convertJSONValueToPyObject(json_object->get(key));
            if (PyDict_SetItemString(py_dict.get(), key.c_str(), py_value.get()) != 0)
                throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to set Python dict item '{}': {}", key, getExceptionMessage());
        }

        return py_dict;
    }

    if (value.type() == typeid(Poco::JSON::Array::Ptr))
    {
        auto json_array = value.extract<Poco::JSON::Array::Ptr>();
        PyObjectPtr py_list{PyList_New(static_cast<Py_ssize_t>(json_array->size()))};
        if (!py_list)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python list for JSON array: {}", getExceptionMessage());

        for (size_t i = 0; i < json_array->size(); ++i)
        {
            PyObjectPtr py_item = convertJSONValueToPyObject(json_array->get(i));
            PyList_SET_ITEM(py_list.get(), static_cast<Py_ssize_t>(i), py_item.release());
        }

        return py_list;
    }

    if (value.type() == typeid(bool))
        return PyObjectPtr{PyBool_FromLong(value.extract<bool>() ? 1 : 0)};

    if (value.type() == typeid(Int8))
        return PyObjectPtr{PyLong_FromLong(value.extract<Int8>())};
    if (value.type() == typeid(Int16))
        return PyObjectPtr{PyLong_FromLong(value.extract<Int16>())};
    if (value.type() == typeid(Int32))
        return PyObjectPtr{PyLong_FromLong(value.extract<Int32>())};
    if (value.type() == typeid(Int64))
        return PyObjectPtr{PyLong_FromLongLong(value.extract<Int64>())};
    if (value.type() == typeid(UInt8))
        return PyObjectPtr{PyLong_FromUnsignedLong(value.extract<UInt8>())};
    if (value.type() == typeid(UInt16))
        return PyObjectPtr{PyLong_FromUnsignedLong(value.extract<UInt16>())};
    if (value.type() == typeid(UInt32))
        return PyObjectPtr{PyLong_FromUnsignedLong(value.extract<UInt32>())};
    if (value.type() == typeid(UInt64))
        return PyObjectPtr{PyLong_FromUnsignedLongLong(value.extract<UInt64>())};
    if (value.type() == typeid(int))
        return PyObjectPtr{PyLong_FromLong(value.extract<int>())};
    if (value.type() == typeid(unsigned int))
        return PyObjectPtr{PyLong_FromUnsignedLong(value.extract<unsigned int>())};
    if (value.type() == typeid(long))
        return PyObjectPtr{PyLong_FromLong(value.extract<long>())};
    if (value.type() == typeid(unsigned long))
        return PyObjectPtr{PyLong_FromUnsignedLong(value.extract<unsigned long>())};

    if (value.type() == typeid(Float32))
        return PyObjectPtr{PyFloat_FromDouble(value.extract<Float32>())};
    if (value.type() == typeid(Float64))
        return PyObjectPtr{PyFloat_FromDouble(value.extract<Float64>())};
    if (value.type() == typeid(double))
        return PyObjectPtr{PyFloat_FromDouble(value.extract<double>())};

    if (value.type() == typeid(String))
    {
        const auto & string_value = value.extract<String>();
        return PyObjectPtr{PyUnicode_FromStringAndSize(string_value.data(), static_cast<Py_ssize_t>(string_value.size()))};
    }

    throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Unsupported JSON value type for Python init config: {}", value.type().name());
}

PyObjectPtr createArgumentsTuple(const Poco::JSON::Object::Ptr & argument)
{
    if (!argument)
        return PyObjectPtr{PyTuple_New(0)};

    PyObjectPtr py_args{PyTuple_New(1)};
    if (!py_args)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python config tuple: {}", getExceptionMessage());

    PyObjectPtr py_arg = convertJSONValueToPyObject(Poco::Dynamic::Var{argument});
    if (PyTuple_SetItem(py_args.get(), 0, py_arg.release()) != 0)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to set Python config tuple item: {}", getExceptionMessage());

    return py_args;
}

PyObjectPtr executeFunction(const std::string & func_name, const std::string & module_name, const Strings & arguments)
{
    auto py_func = getFunction(func_name, module_name);
    auto py_args = createArgumentsTuple(arguments);
    return executeObject(py_func, py_args);
}

PyObjectPtr executeFunction(const std::string & func_name, const std::string & module_name, const PyObjectPtr & args)
{
    auto py_func = getFunction(func_name, module_name);
    return executeObject(py_func, args);
}

bool isGenerator(const PyObjectPtr & obj)
{
    if (!obj)
        return false;

    /// Check if it's a generator object (created by yield)
    if (PyGen_Check(obj.get()))
        return true;

    /// Check if it has __next__ method (iterator protocol)
    if (PyIter_Check(obj.get()))
        return true;

    return false;
}

bool isAsyncGeneratorOrCoroutine(const PyObjectPtr & obj)
{
    if (!obj)
        return false;

    return PyCoro_CheckExact(obj.get()) || PyAsyncGen_CheckExact(obj.get());
}

bool isIterable(const PyObjectPtr & obj)
{
    if (!obj)
        return false;

    /// PyObject_HasAttrStringWithError (3.13+) replaces PyObject_HasAttrString
    /// which silently swallows exceptions from __getattribute__ and prints
    /// a noisy deprecation warning in 3.14+.
    int has_iter = PyObject_HasAttrStringWithError(obj.get(), "__iter__");
    if (has_iter < 0)
    {
        PyErr_Clear();
        return false;
    }

    return has_iter || PySequence_Check(obj.get());
}

PyObjectPtr getIterator(const PyObjectPtr & obj)
{
    if (!obj)
        return PyObjectPtr{};

    PyObjectPtr iterator{PyObject_GetIter(obj.get())};

    if (!iterator)
    {
        if (hasException())
        {
            std::string error_message = getExceptionMessage();
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to get iterator: {}", error_message);
        }
        return PyObjectPtr{};
    }

    return iterator;
}

PyObjectPtr iterNext(const PyObjectPtr & iterator)
{
    if (!iterator)
        return PyObjectPtr{};

    PyObjectPtr next_item{PyIter_Next(iterator.get())};

    /// PyIter_Next returns NULL when exhausted (no exception) or on error (with exception)
    if (!next_item)
    {
        if (hasException())
        {
            if (PyErr_ExceptionMatches(PyExc_KeyboardInterrupt) || PyErr_ExceptionMatches(PyExc_GeneratorExit))
            {
                PyErr_Clear();
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
            }
            std::string error_message = getExceptionMessage();
            throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Iterator error: {}", error_message);
        }
        /// Iterator exhausted - return empty pointer
        return PyObjectPtr{};
    }

    return next_item;
}

PyObjectPtr normalizePythonListForTuple(const PyObjectPtr & py_list, size_t tuple_size)
{
    if (!py_list)
        return PyObjectPtr::borrow(py_list.get());

    auto make_list_from_iterable = [](PyObject * obj) {
        PyObjectPtr list{PySequence_List(obj)};
        if (!list)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert Python iterable to list: {}", getExceptionMessage());
        return list;
    };

    auto make_list_from_row = [](PyObject * row) {
        PyObjectPtr list{PyList_New(1)};
        if (!list)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python list for tuple normalization");
        Py_INCREF(row);
        PyList_SET_ITEM(list.get(), 0, row);
        return list;
    };

    auto make_list_from_scalar = [](PyObject * value) {
        PyObjectPtr list{PyList_New(1)};
        if (!list)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python list for tuple normalization");
        PyObject * tuple = PyTuple_New(1);
        if (!tuple)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python tuple for tuple normalization");
        Py_INCREF(value);
        PyTuple_SET_ITEM(tuple, 0, value);
        PyList_SET_ITEM(list.get(), 0, tuple);
        return list;
    };

    auto is_string_like = [](PyObject * obj) { return PyUnicode_Check(obj) || PyBytes_Check(obj) || PyByteArray_Check(obj); };

    if (PyList_Check(py_list.get()))
    {
        if (tuple_size != 1)
            return PyObjectPtr::borrow(py_list.get());

        Py_ssize_t size = PyList_Size(py_list.get());
        if (size == 0)
            return PyObjectPtr::borrow(py_list.get());

        PyObject * first = PyList_GetItem(py_list.get(), 0);
        if (PyList_Check(first) || PyTuple_Check(first))
            return PyObjectPtr::borrow(py_list.get());

        PyObjectPtr new_list{PyList_New(size)};
        if (!new_list)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python list for tuple normalization");

        for (Py_ssize_t i = 0; i < size; ++i)
        {
            PyObject * item = PyList_GetItem(py_list.get(), i);
            PyObject * tuple = PyTuple_New(1);
            if (!tuple)
                throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to allocate Python tuple for tuple normalization");
            Py_INCREF(item);
            PyTuple_SET_ITEM(tuple, 0, item);
            PyList_SET_ITEM(new_list.get(), i, tuple);
        }

        return new_list;
    }

    if (PyTuple_Check(py_list.get()))
    {
        Py_ssize_t size = PyTuple_Size(py_list.get());
        if (size == 0)
            return PyObjectPtr{PyList_New(0)};

        if (tuple_size == 1)
        {
            auto list = make_list_from_iterable(py_list.get());
            return normalizePythonListForTuple(list, tuple_size);
        }

        PyObject * first = PyTuple_GetItem(py_list.get(), 0);
        if (first && (PyList_Check(first) || PyTuple_Check(first)))
            return make_list_from_iterable(py_list.get());

        return make_list_from_row(py_list.get());
    }

    if (tuple_size == 1)
    {
        if (!is_string_like(py_list.get()) && isIterable(py_list))
        {
            auto list = make_list_from_iterable(py_list.get());
            return normalizePythonListForTuple(list, tuple_size);
        }
        return make_list_from_scalar(py_list.get());
    }

    if (!is_string_like(py_list.get()) && isIterable(py_list))
        return make_list_from_iterable(py_list.get());

    return PyObjectPtr::borrow(py_list.get());
}
}
