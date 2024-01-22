# Pipez - lightweight library for fast deploy stream handling

## Install

For installing default version of library use

```
pip install pipez
```

If you want install specific version pipez - use

```
pip install pipez[<your choice>]
```

Now available `cv` and `onnxruntime` versions.


## Quick start

### Developing custom node

If you want use your node - you can use `Registry.add` as class decorator
from `pipez.registry`. You should also import base `Node`
class from `pipez.node`. For example:

```python
from pipez.node import  Node
from pipez.registry import Registry

Registry.add
class MyNode(Node):
    ...
```

Once required method which you should override: `work_func(...)` which
handle `Batch` from `pipez.batch`. However, methods
`post_init(...)` and `close(...)` also available. See next example:

```python
from typing import Optional

from pipez.batch import Batch, BatchStatus
from pipez.node import  Node
from pipez.registry import Registry


Registry.add
class MyNode(Node):
    def __init__(
            self,
            a: int = 1,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._a = a

    def post_init(self):
        self._a *= 10

    def close(self):
        self._a = 0
    
    def work_func(
            self,
            data: Optional[Batch] = None
    ) -> Batch:
        self._a *= 2
        if self._a > 1000:
            return Batch(status=BatchStatus.END)
        return Batch(data=[dict(a=self._a)])
```

### Build pipelines

When you defined all nodes what you need, we build pipeline from them.
You can use json describe or class for node. See next examples:

For using json describing you must add `Registry.add` as class decorator
for you node, else you will get error.
```python
{
    "cls": "MyNode",
    "type": "Process",
    "output": "some_trash"
}
```

For using class you must import your node class.
```python
from pipez.node import NodeType

from ... import MyNode


MyNode(
    a=5,
    type=NodeType.PROCESS,
    output='some_trash'
)
```

As we can see, we used `NodeType`, which define type of node.

For building pipeline, we must use `build_pipeline` from `pipez.build`.
For example:

```python
from pipez.build import build_pipeline
from pipez.nodes import DummyNode
from pipez.node import NodeType
from ... import MyNode

watchdog = build_pipeline(
    pipeline=[
        MyNode(
            a=10,
            type=NodeType.THREAD,
            output='q1'
        ),
        DummyNode(
            type=NodeType.PROCESS,
            input='q1',
            output='q2'
        ),
        DummyNode(
            type=NodeType.THREAD,
            input=['q1, q2'],
            output='q3'
        ),
        {
            "cls": DummyNode,
            "type": "thread",
            "input": "q3"
        }
    ]
)
```

As we can see, `build_pipeline` return `watchdog`.
You can read about it in next section.


### WatchDog

TODO

## Сontributors

- Alexander, https://github.com/tam2511
- Vitaly, https://github.com/purple63
