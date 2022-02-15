def func1(a,b):
    print(f"a = {a}")
    print(f"b = {b}")

def func(func_name, func_args):
    func_name(*func_args)

func_args = (1, 2)

func(func1, func_args)
