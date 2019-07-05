def dec(a=True):
    def wrapper(func):
        def foo():
            print('meh')
            return func()

        return foo if a else func

    return wrapper


@dec(a=False)
def meh():
    print('meh')

@dec()
def ehh():
    print('ehh')


meh()
ehh()
