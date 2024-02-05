from rolecraft import Config, role
from rolecraft.broker import StubBroker

Config().set_default(broker=StubBroker()).inject()


@role
def add(a, b):
    print(f"result of {a} + {b} is {a + b}")
    return a + b


if __name__ == "__main__":
    add.dispatch_message(1, 2)
