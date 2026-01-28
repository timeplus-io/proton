#include "config.h"

#if USE_PYTHON_UDF
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <Common/Exception.h>

#include <CPython/tests/CPythonTest.h>


namespace DB::ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_RUNNING_ERROR;
}

using namespace DB::cpython;


TEST_F(CPythonTest, convertPyObjectToString)
{
    assertNoLeak([]() {
        auto pstr = PyObjectPtr{PyUnicode_FromString("str")};
        EXPECT_EQ(convertPyObjectToString(pstr), "str");
        EXPECT_EQ(pstr->ob_refcnt, 1);
    });
}

TEST_F(CPythonTest, convertPyObjectToStringException)
{
    const char * class_def = R"(
class FaultClass:
    def __str__(self):
        raise RuntimeError("Simulated error in __str__")    
)";

    PyRun_SimpleString(class_def);

    assertNoLeak([&]() {
        auto py_class = getClass("FaultClass", "__main__");

        EXPECT_TRUE(py_class);

        auto fault_instance = newInstance(py_class);

        EXPECT_TRUE(fault_instance);

        try
        {
            convertPyObjectToString(fault_instance);
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
            EXPECT_STREQ(
                e.what(),
                "Convert Python varible to string error, detail message : Simulated error in __str__: \nFile \"<string>\", line 4, in "
                "__str__\n\n");
        }
    });
}

TEST_F(CPythonTest, hasNotException)
{
    assertNoLeak([]() { EXPECT_FALSE(hasException()); });
}

TEST_F(CPythonTest, hasException)
{
    assertNoLeak([]() {
        PyErr_BadArgument(); /// Raise a python exception

        EXPECT_TRUE(hasException());

        PyErr_Clear();
    });
}

TEST_F(CPythonTest, getExceptionMessage)
{
    assertNoLeak([]() {
        PyErr_BadArgument(); /// Raise a python exception

        EXPECT_TRUE(hasException());

        /// getExceptionMessage will catch exception and clear the exception flag,
        /// so there is no need to call PyErr_Clear after this line
        EXPECT_EQ(getExceptionMessage(), "bad argument type for built-in operation: \n");

        EXPECT_FALSE(hasException());
    });
}

TEST_F(CPythonTest, isIterableClearsPyObjectHasAttrError)
{
    const char * class_def = R"(
class FaultGetAttribute:
    def __getattribute__(self, name):
        raise RuntimeError("Simulated __getattribute__ failure")
)";

    PyRun_SimpleString(class_def);

    assertNoLeak([&]() {
        auto py_class = getClass("FaultGetAttribute", "__main__");
        EXPECT_TRUE(py_class);

        auto instance = newInstance(py_class);
        EXPECT_TRUE(instance);

        EXPECT_FALSE(isIterable(instance));
        EXPECT_FALSE(hasException());
    });
}

TEST_F(CPythonTest, getAttr)
{
    assertNoLeak([]() {
        auto pmodule = PyObjectPtr{PyModule_New("test_module")};

        PyObject * py_value = PyUnicode_FromString("value");

        {
            /// To ensure that this object is only owned by py_module
            auto pvalue = PyObjectPtr{py_value};
            PyObject_SetAttrString(pmodule.get(), "value", py_value);
        }

        EXPECT_EQ(py_value->ob_refcnt, 1);

        {
            auto pattr = getAttr(pmodule, "value");
            EXPECT_EQ(pattr.get(), py_value);

            /// One reference is owned by pattr, the other is owned by py_module
            EXPECT_EQ(pattr->ob_refcnt, 2);
        }

        EXPECT_EQ(py_value->ob_refcnt, 1);
    });
}

TEST_F(CPythonTest, getAttrNotFound)
{
    assertNoLeak([]() {
        PyObject * py_module = PyModule_New("test_module");
        auto pmodule = PyObjectPtr{py_module};

        try
        {
            getAttr(pmodule, "value");
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
            EXPECT_STREQ(e.what(), "Failed to get attribute: module 'test_module' has no attribute 'value': \n");
        }
    });
}

TEST_F(CPythonTest, tryGetAttr)
{
    assertNoLeak([]() {
        auto pmodule = PyObjectPtr{PyModule_New("test_module")};

        PyObject * py_value = PyUnicode_FromString("value");

        {
            /// To ensure that this object is only owned by py_module
            auto pvalue = PyObjectPtr{py_value};
            PyObject_SetAttrString(pmodule.get(), "value", py_value);
        }

        EXPECT_EQ(py_value->ob_refcnt, 1);

        {
            auto pattr = tryGetAttr(pmodule, "value");
            EXPECT_EQ(pattr.get(), py_value);

            /// One reference is owned by pattr, the other is owned by py_module
            EXPECT_EQ(pattr->ob_refcnt, 2);
        }

        EXPECT_EQ(py_value->ob_refcnt, 1);
    });
}

TEST_F(CPythonTest, tryGetAttrNotFound)
{
    assertNoLeak([&]() {
        PyObject * py_module = PyModule_New("test_module");
        auto pmodule = PyObjectPtr{py_module};

        auto pattr = tryGetAttr(pmodule, "value");
        EXPECT_FALSE(pattr);
        EXPECT_FALSE(hasException());

        /// PyModule_New doesn't import the module,
        /// we can't find it in sys.modules
        EXPECT_FALSE(hasModule("test_module"));
        EXPECT_EQ(py_module->ob_refcnt, 1);
    });
}

TEST_F(CPythonTest, importModule)
{
    /// When importing a module, it may cause chained imports.
    /// We are unable to release all the module objects that this module depends on.
    /// This may result in a slight memory leak.
    /// However, because the amount is small and `sys.modules` is a dictionary that
    /// deduplicates these objects, the memory leak will not continuously grow in the
    /// actual runtime environment.
    auto pmodule = importModule("sys");

    EXPECT_TRUE(pmodule);
}

TEST_F(CPythonTest, importModuleFailed)
{
    try
    {
        auto pmodule = importModule("proton_not_exist");
        FAIL() << "Expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
        EXPECT_STREQ(e.what(), "Failed to import module proton_not_exist: No module named 'proton_not_exist': \n");
    }
}

TEST_F(CPythonTest, addModule)
{
    assertNoLeak([]() {
        std::string module_name = "proton_module";
        auto m = getOrAddModule(module_name);

        EXPECT_TRUE(m);

        EXPECT_TRUE(hasModule(module_name));

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, unloadModuleNotImport)
{
    assertNoLeak([]() {
        std::string module_name = "proton_not_exist";
        try
        {
            unloadModule(module_name);
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
            EXPECT_STREQ(e.what(), "Failed to unload module: proton_not_exist");
        }
    });
}

TEST_F(CPythonTest, getModuleFound)
{
    assertNoLeak([]() {
        std::string module_name = "proton_module";
        auto m = getOrAddModule(module_name);

        EXPECT_TRUE(m);

        EXPECT_TRUE(hasModule(module_name));

        auto pmodule = getModule(module_name);
        EXPECT_TRUE(pmodule);

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, getModuleNotFound)
{
    auto pmodule = getModule("proton_not_exist");

    EXPECT_FALSE(pmodule);
}

TEST_F(CPythonTest, hasModule)
{
    EXPECT_TRUE(hasModule("sys"));
}

TEST_F(CPythonTest, hasModuleNotFound)
{
    EXPECT_FALSE(hasModule("proton_not_exist"));
}


TEST_F(CPythonTest, getObjectTypeBuiltin)
{
    assertNoLeak([]() {
        auto pstr = PyObjectPtr{PyUnicode_FromString("str")};

        EXPECT_EQ(getObjectType(pstr), "<class 'str'>");
    });
}

TEST_F(CPythonTest, getObjectTypeClass)
{
    assertNoLeak([&]() {
        const char * class_def = R"(
class TestClass:
    def __str__(self):
        raise RuntimeError("Simulated error in __str__")    
)";

        PyRun_SimpleString(class_def);

        auto py_class = getClass("TestClass", "__main__");

        EXPECT_TRUE(py_class);

        auto py_instance = newInstance(py_class);

        EXPECT_TRUE(py_instance);
        EXPECT_EQ(getObjectType(py_instance), "<class '__main__.TestClass'>");

        unloadModule("__main__");
        unloadClass(py_class);
    });
}

TEST_F(CPythonTest, getClassNotFound)
{
    assertNoLeak([]() {
        try
        {
            getClass("TestClassNotFound", "__main__");
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
            EXPECT_STREQ(e.what(), "The python class name is not the same as the user defined function name: TestClassNotFound");
        }
        unloadModule("__main__");
    });
}

TEST_F(CPythonTest, getObjectTypeFunction)
{
    assertNoLeak([&]() {
        const char * func_def = R"(
def proton_func():
    pass
)";

        PyRun_SimpleString(func_def);

        auto py_func = getFunction("proton_func", "__main__");

        EXPECT_TRUE(py_func);

        EXPECT_EQ(getObjectType(py_func), "<class 'function'>");

        unloadModule("__main__");
    });
}

TEST_F(CPythonTest, getFunctionNotFound)
{
    assertNoLeak([]() {
        try
        {
            getFunction("proton_func_not_found", "__main__");
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
            EXPECT_STREQ(e.what(), "The python function name is not the same as the user defined function name: proton_func_not_found");
        }

        unloadModule("__main__");
    });
}

TEST_F(CPythonTest, getInstanceMethod)
{
    assertNoLeak([&]() {
        const char * class_def = R"(
class TestClass:
    def method(self, i):
        pass
)";

        PyRun_SimpleString(class_def);

        auto py_class = getClass("TestClass", "__main__");

        EXPECT_TRUE(py_class);

        auto py_instance = newInstance(py_class);

        EXPECT_TRUE(py_instance);

        auto method = getInstanceMethod(py_instance, "method");
        EXPECT_TRUE(method);

        unloadClass(py_class);
        unloadModule("__main__");
    });
}

TEST_F(CPythonTest, getInstanceMethodNotFound)
{
    assertNoLeak([&]() {
        const char * class_def = R"(
class TestClass:
    def method(self, i):
        pass
)";

        PyRun_SimpleString(class_def);

        auto py_class = getClass("TestClass", "__main__");

        EXPECT_TRUE(py_class);

        auto py_instance = newInstance(py_class);

        EXPECT_TRUE(py_instance);

        try
        {
            getInstanceMethod(py_instance, "method_not_exist");
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_INTERNAL_ERROR);
            EXPECT_STREQ(e.what(), "The python class does not have the method: method_not_exist");
        }

        unloadClass(py_class);
        unloadModule("__main__");
    });
}

TEST_F(CPythonTest, compile)
{
    assertNoLeak([]() {
        auto byte_code = compile("print('Hello, World!')");

        EXPECT_TRUE(byte_code);
    });
}

TEST_F(CPythonTest, compileFunc)
{
    assertNoLeak([]() {
        const char * func_def = R"(
def func(i):
    return i + 5
)";

        auto byte_code = compile(func_def);
        EXPECT_TRUE(byte_code);
    });
}

TEST_F(CPythonTest, compileError)
{
    assertNoLeak([]() {
        try
        {
            compile("not valid python code");
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_COMPILE_ERROR);
            EXPECT_STREQ(e.what(), "the Python UDF is invalid. Detail error: invalid syntax (, line 1): \n");
        }
    });
}

TEST_F(CPythonTest, executeByteCode)
{
    assertNoLeak([]() {
        const char * script = R"(
y = 10
x = y + 20
x)";
        auto byte_code = compile(script);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto result = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(result);
        EXPECT_TRUE(Py_IsNone(result.get()));

        auto main_module = getOrAddModule(module_name);
        auto x = getAttr(main_module, "x");
        auto y = getAttr(main_module, "y");

        EXPECT_EQ(PyLong_AsLong(x.get()), 30);
        EXPECT_EQ(PyLong_AsLong(y.get()), 10);

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, executeByteCodeFunc)
{
    assertNoLeak([]() {
        const char * func_def = R"(
def func(i):
    return i + 5
)";

        auto byte_code = compile(func_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);
        EXPECT_TRUE(Py_IsNone(exec_res.get()));

        auto main_module = getOrAddModule(module_name);

        /// executeByteCode use module dict as global dict,
        /// it will reference function or class object in it
        /// so we need to unload the module to clear the dict
        /// to decrease the reference count of function or class object
        /// so that these kind of objects can be released
        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, executeByteCodeThrowException)
{
    assertNoLeak([]() {
        const char * script = R"(
raise Exception("error")
)";
        auto byte_code = compile(script);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        try
        {
            executeByteCode(byte_code, module_name);
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_RUNNING_ERROR);
            EXPECT_STREQ(e.what(), "UDF running error, detail message: error: \nFile \"\", line 2, in <module>\n\n");
        }
        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, executeObject)
{
    assertNoLeak([]() {
        const char * func_def = R"(
def func(i):
    return i + 5
)";

        auto byte_code = compile(func_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);
        EXPECT_TRUE(Py_IsNone(exec_res.get()));

        auto main_module = getOrAddModule(module_name);

        auto py_func = getFunction("func", module_name);
        EXPECT_TRUE(py_func);

        auto args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(args.get(), 0, PyLong_FromLong(10));

        auto result = executeObject(py_func, args);
        EXPECT_TRUE(result);
        EXPECT_EQ(PyLong_AsLong(result.get()), 15);

        unloadModule(module_name);

        EXPECT_EQ(args->ob_refcnt, 1);
        EXPECT_EQ(py_func->ob_refcnt, 1);
        EXPECT_EQ(main_module->ob_refcnt, 1);
        EXPECT_EQ(byte_code->ob_refcnt, 1);
    });
}

TEST_F(CPythonTest, executeObjectNoReturn)
{
    assertNoLeak([]() {
        const char * func_def = R"(
def func(i):
    pass
)";

        auto byte_code = compile(func_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);
        EXPECT_TRUE(Py_IsNone(exec_res.get()));

        auto main_module = getOrAddModule(module_name);

        auto py_func = getFunction("func", module_name);
        EXPECT_TRUE(py_func);

        auto args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(args.get(), 0, PyLong_FromLong(10));

        auto result = executeObject(py_func, args);
        EXPECT_TRUE(result);
        EXPECT_TRUE(Py_IsNone(result.get()));

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, executeMethod)
{
    assertNoLeak([]() {
        const char * class_def = R"(
class TestClass:
    def method(self, i):
        return i + 5
)";

        auto byte_code = compile(class_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto load_class = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(load_class);

        auto main_module = getOrAddModule(module_name);

        auto py_class = getClass("TestClass", module_name);
        EXPECT_TRUE(py_class);

        auto py_instance = newInstance(py_class);
        EXPECT_TRUE(py_instance);

        auto py_method = getInstanceMethod(py_instance, "method");
        EXPECT_TRUE(py_method);

        auto args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(args.get(), 0, PyLong_FromLong(10));

        auto result = executeObject(py_method, args);
        EXPECT_TRUE(result);
        EXPECT_EQ(PyLong_AsLong(result.get()), 15);

        unloadModule(module_name);

        /// class object has some attribute which reference itself,
        /// it will cause reference cycle, so we need to unload the class
        /// to clear `__dict__`, `__mro__`, ``__bases__`
        /// And then the py class object can be released
        unloadClass(py_class);
    });
}

TEST_F(CPythonTest, executeObjectNotCallable)
{
    assertNoLeak([]() {
        const char * class_def = R"(
x = 5
)";

        auto byte_code = compile(class_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto load_class = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(load_class);

        auto main_module = getOrAddModule(module_name);
        auto x = getAttr(main_module, "x");

        try
        {
            executeObject(x);
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_RUNNING_ERROR);
            EXPECT_STREQ(e.what(), "UDF running error, detail message: 'int' object is not callable: \n");
        }

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, executeObjectThrowException)
{
    assertNoLeak([]() {
        const char * func_def = R"(
def func(i):
    raise Exception("timeplus udf error")
)";

        auto byte_code = compile(func_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);
        EXPECT_TRUE(Py_IsNone(exec_res.get()));

        auto main_module = getOrAddModule(module_name);

        auto py_func = getFunction("func", module_name);
        EXPECT_TRUE(py_func);

        auto args = PyObjectPtr{PyTuple_New(1)};
        PyTuple_SetItem(args.get(), 0, PyLong_FromLong(10));

        try
        {
            executeObject(py_func, args);
            FAIL() << "Expected DB::Exception";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_RUNNING_ERROR);
            EXPECT_STREQ(e.what(), "UDF running error, detail message: timeplus udf error: \nFile \"\", line 3, in func\n\n");
        }

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, isGeneratorWithGenerator)
{
    assertNoLeak([]() {
        const char * gen_def = R"(
def gen_func():
    yield 1
    yield 2
    yield 3
)";

        auto byte_code = compile(gen_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);

        auto py_func = getFunction("gen_func", module_name);
        EXPECT_TRUE(py_func);

        /// Call the generator function to get a generator object
        auto generator = executeObject(py_func);
        EXPECT_TRUE(generator);

        /// The result should be a generator
        EXPECT_TRUE(isGenerator(generator));

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, isGeneratorWithList)
{
    assertNoLeak([]() {
        auto py_list = PyObjectPtr{PyList_New(3)};
        PyList_SetItem(py_list.get(), 0, PyLong_FromLong(1));
        PyList_SetItem(py_list.get(), 1, PyLong_FromLong(2));
        PyList_SetItem(py_list.get(), 2, PyLong_FromLong(3));

        /// A list is not a generator
        EXPECT_FALSE(isGenerator(py_list));
    });
}

TEST_F(CPythonTest, isGeneratorWithIterator)
{
    assertNoLeak([]() {
        auto py_list = PyObjectPtr{PyList_New(3)};
        PyList_SetItem(py_list.get(), 0, PyLong_FromLong(1));
        PyList_SetItem(py_list.get(), 1, PyLong_FromLong(2));
        PyList_SetItem(py_list.get(), 2, PyLong_FromLong(3));

        /// Get iterator from list
        auto iterator = getIterator(py_list);
        EXPECT_TRUE(iterator);

        /// An iterator should be detected as generator-like
        EXPECT_TRUE(isGenerator(iterator));
    });
}

TEST_F(CPythonTest, isIterable)
{
    assertNoLeak([]() {
        auto py_list = PyObjectPtr{PyList_New(0)};
        EXPECT_TRUE(isIterable(py_list));

        auto py_int = PyObjectPtr{PyLong_FromLong(42)};
        EXPECT_FALSE(isIterable(py_int));
    });
}

TEST_F(CPythonTest, getIterator)
{
    assertNoLeak([]() {
        auto py_list = PyObjectPtr{PyList_New(3)};
        PyList_SetItem(py_list.get(), 0, PyLong_FromLong(10));
        PyList_SetItem(py_list.get(), 1, PyLong_FromLong(20));
        PyList_SetItem(py_list.get(), 2, PyLong_FromLong(30));

        auto iterator = getIterator(py_list);
        EXPECT_TRUE(iterator);
    });
}

TEST_F(CPythonTest, iterNextBasic)
{
    assertNoLeak([]() {
        auto py_list = PyObjectPtr{PyList_New(3)};
        PyList_SetItem(py_list.get(), 0, PyLong_FromLong(10));
        PyList_SetItem(py_list.get(), 1, PyLong_FromLong(20));
        PyList_SetItem(py_list.get(), 2, PyLong_FromLong(30));

        auto iterator = getIterator(py_list);
        EXPECT_TRUE(iterator);

        /// First item
        auto item1 = iterNext(iterator);
        EXPECT_TRUE(item1);
        EXPECT_EQ(PyLong_AsLong(item1.get()), 10);

        /// Second item
        auto item2 = iterNext(iterator);
        EXPECT_TRUE(item2);
        EXPECT_EQ(PyLong_AsLong(item2.get()), 20);

        /// Third item
        auto item3 = iterNext(iterator);
        EXPECT_TRUE(item3);
        EXPECT_EQ(PyLong_AsLong(item3.get()), 30);

        /// Iterator exhausted
        auto item4 = iterNext(iterator);
        EXPECT_FALSE(item4);
    });
}

TEST_F(CPythonTest, iterNextWithGenerator)
{
    assertNoLeak([]() {
        const char * gen_def = R"(
def gen_func():
    yield 100
    yield 200
    yield 300
)";

        auto byte_code = compile(gen_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);

        auto py_func = getFunction("gen_func", module_name);
        EXPECT_TRUE(py_func);

        /// Call the generator function to get a generator object
        auto generator = executeObject(py_func);
        EXPECT_TRUE(generator);
        EXPECT_TRUE(isGenerator(generator));

        /// Iterate through generator
        auto item1 = iterNext(generator);
        EXPECT_TRUE(item1);
        EXPECT_EQ(PyLong_AsLong(item1.get()), 100);

        auto item2 = iterNext(generator);
        EXPECT_TRUE(item2);
        EXPECT_EQ(PyLong_AsLong(item2.get()), 200);

        auto item3 = iterNext(generator);
        EXPECT_TRUE(item3);
        EXPECT_EQ(PyLong_AsLong(item3.get()), 300);

        /// Generator exhausted
        auto item4 = iterNext(generator);
        EXPECT_FALSE(item4);

        unloadModule(module_name);
    });
}

TEST_F(CPythonTest, normalizePythonListForTupleListOfScalars)
{
    assertNoLeak([]() {
        auto py_list = PyObjectPtr{PyList_New(2)};
        PyList_SetItem(py_list.get(), 0, PyLong_FromLong(10));
        PyList_SetItem(py_list.get(), 1, PyLong_FromLong(20));

        auto normalized = normalizePythonListForTuple(py_list, 1);
        EXPECT_TRUE(PyList_Check(normalized.get()));
        EXPECT_EQ(PyList_Size(normalized.get()), 2);

        auto * first = PyList_GetItem(normalized.get(), 0);
        ASSERT_TRUE(PyTuple_Check(first));
        EXPECT_EQ(PyTuple_Size(first), 1);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(first, 0)), 10);
    });
}

TEST_F(CPythonTest, normalizePythonListForTupleScalarSingleColumn)
{
    assertNoLeak([]() {
        auto py_value = PyObjectPtr{PyLong_FromLong(42)};
        auto normalized = normalizePythonListForTuple(py_value, 1);
        EXPECT_TRUE(PyList_Check(normalized.get()));
        EXPECT_EQ(PyList_Size(normalized.get()), 1);

        auto * item = PyList_GetItem(normalized.get(), 0);
        ASSERT_TRUE(PyTuple_Check(item));
        EXPECT_EQ(PyTuple_Size(item), 1);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(item, 0)), 42);
    });
}

TEST_F(CPythonTest, normalizePythonListForTupleTupleRow)
{
    assertNoLeak([]() {
        auto py_row = PyObjectPtr{PyTuple_New(2)};
        PyTuple_SetItem(py_row.get(), 0, PyLong_FromLong(1));
        PyTuple_SetItem(py_row.get(), 1, PyLong_FromLong(2));

        auto normalized = normalizePythonListForTuple(py_row, 2);
        EXPECT_TRUE(PyList_Check(normalized.get()));
        EXPECT_EQ(PyList_Size(normalized.get()), 1);

        auto * item = PyList_GetItem(normalized.get(), 0);
        ASSERT_TRUE(PyTuple_Check(item));
        EXPECT_EQ(PyTuple_Size(item), 2);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(item, 0)), 1);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(item, 1)), 2);
    });
}

TEST_F(CPythonTest, normalizePythonListForTupleTupleOfRows)
{
    assertNoLeak([]() {
        auto py_row1 = PyObjectPtr{PyTuple_New(2)};
        PyTuple_SetItem(py_row1.get(), 0, PyLong_FromLong(1));
        PyTuple_SetItem(py_row1.get(), 1, PyLong_FromLong(2));

        auto py_row2 = PyObjectPtr{PyTuple_New(2)};
        PyTuple_SetItem(py_row2.get(), 0, PyLong_FromLong(3));
        PyTuple_SetItem(py_row2.get(), 1, PyLong_FromLong(4));

        auto py_rows = PyObjectPtr{PyTuple_New(2)};
        PyTuple_SetItem(py_rows.get(), 0, py_row1.release());
        PyTuple_SetItem(py_rows.get(), 1, py_row2.release());

        auto normalized = normalizePythonListForTuple(py_rows, 2);
        EXPECT_TRUE(PyList_Check(normalized.get()));
        EXPECT_EQ(PyList_Size(normalized.get()), 2);

        auto * item = PyList_GetItem(normalized.get(), 1);
        ASSERT_TRUE(PyTuple_Check(item));
        EXPECT_EQ(PyTuple_Size(item), 2);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(item, 0)), 3);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(item, 1)), 4);
    });
}

TEST_F(CPythonTest, normalizePythonListForTupleIterableSingleColumn)
{
    assertNoLeak([]() {
        auto range_obj = PyObjectPtr{PyObject_CallFunction(reinterpret_cast<PyObject *>(&PyRange_Type), "ii", 0, 3)};
        ASSERT_TRUE(range_obj);

        auto normalized = normalizePythonListForTuple(range_obj, 1);
        EXPECT_TRUE(PyList_Check(normalized.get()));
        EXPECT_EQ(PyList_Size(normalized.get()), 3);

        auto * item = PyList_GetItem(normalized.get(), 2);
        ASSERT_TRUE(PyTuple_Check(item));
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(item, 0)), 2);
    });
}

TEST_F(CPythonTest, streamingPythonTableSimulation)
{
    /// This test simulates how a streaming Python table would work
    assertNoLeak([]() {
        const char * gen_def = R"(
def streaming_source():
    """Simulates a streaming data source that yields batches"""
    for batch_num in range(3):
        # Each batch is a list of tuples (representing rows)
        batch = [(batch_num * 10 + i, f"item_{batch_num}_{i}") for i in range(2)]
        yield batch
)";

        auto byte_code = compile(gen_def);
        EXPECT_TRUE(byte_code);

        auto module_name = randomModuleName();

        auto exec_res = executeByteCode(byte_code, module_name);
        EXPECT_TRUE(exec_res);

        auto py_func = getFunction("streaming_source", module_name);
        EXPECT_TRUE(py_func);

        auto generator = executeObject(py_func);
        EXPECT_TRUE(generator);
        EXPECT_TRUE(isGenerator(generator));

        /// Collect all batches
        int batch_count = 0;
        while (auto batch = iterNext(generator))
        {
            EXPECT_TRUE(PyList_Check(batch.get()));
            EXPECT_EQ(PyList_Size(batch.get()), 2); /// 2 rows per batch
            batch_count++;
        }

        EXPECT_EQ(batch_count, 3);

        unloadModule(module_name);
    });
}
#endif
