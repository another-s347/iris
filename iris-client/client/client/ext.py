from typing import Any, List, Optional
import torch

from torch.nn.modules import module
from .client import IrisContextInternal, IrisObjectId, IrisObjectInternal, IrisClientInternal
import dill
import sys
import functools
from torch import optim
import asyncio
from inspect import Traceback, getframeinfo, stack
import functools
import contextvars
import threading
import copy
from typing import TypeVar

T = TypeVar('T')


def proxy(module: T, ctx: 'IrisContext') -> T:
    return ProxyModule(module, ctx)


class ProxyModule:
    def __init__(self, module, ctx: 'IrisContext', node: Optional['IrisNode'] = None):
        self.module = module
        self.ctx: 'IrisContext' = ctx
        self.node = node

    def __getattr__(self, name):
        # Check
        # return getattr(self.module, name)
        return ProxyAttr(self.module, getattr(self.module, name), self.ctx, self.node)


class ProxyAttr:
    def __init__(self, module, obj, ctx: 'IrisContext', node: Optional['IrisNode'] = None):
        self.module = module
        self.object = obj
        self.ctx = ctx
        self.node = node

    def __call__(self, *args, **kwargs):
        node = self.node if self.node else self.ctx.control_context.get().current_node
        if node is None:
            raise NotImplementedError()
            # return self.object(*args, **kwargs)
        if node.ip == "local":
            r_args, holds_ref = retrieve_args(node, self.ctx, args)
            obj = self.object(*r_args, **kwargs)
            return IrisObject(obj, node, node.ctx, None, None, i_stack=2)
        if self.object.__module__:
            return node.create_object_by_name(
                self.object.__module__,
                self.object.__qualname__,
                *args,
                **kwargs,
            )
        else:
            return node.create_object_by_name(
                self.module.__name__,
                self.object.__name__,
                *args,
                **kwargs
            )

    def __getattr__(self, name):
        object = getattr(self.object, name)
        return ProxyAttr(self.module, object, self.ctx, self.node)


class OnContext:
    def __init__(self, node: 'IrisNode'):
        self.node: 'IrisNode' = node
        self.current_cid = None

    def __enter__(self):
        cc = self.node.ctx.control_context.get()
        self.last_node = cc.current_node
        cc.current_node = self.node
        self.current_cid = cc.cid

    def __exit__(self, exc_type, exc_value, exc_tracebackc):
        cc = self.node.ctx.control_context.get()
        assert self.current_cid == cc.cid
        cc.current_node = self.last_node

    def __call__(self, module):
        # if isinstance(module)
        if not module.__module__:
            return ProxyModule(module, self.node.ctx, self.node)
        else:
            return ProxyAttr(None, module, self.node.ctx,  self.node)


def on(node):
    return OnContext(node)


class ControlContext:
    def __init__(self, cid):
        self.last_task = []
        self.cid = cid
        self.current_node: Optional[IrisNode] = None

    def set_last_task(self, task):
        self.last_task = [task]

    def get_last_task(self):
        return self.last_task


class IrisConfig:
    go_async: bool = True
    go_async_sequence: bool = True
    debug: bool = True
    log_color: bool = True


class IrisContext:
    def __init__(self, config: Optional[IrisConfig] = None):
        super().__init__()
        self.inner: IrisContextInternal = IrisContextInternal(config)
        self.client_wrapper: dict[str, IrisNode] = {}
        self.config: IrisConfig = config if config else IrisConfig()
        self.ports = 12345
        self.control_context: contextvars.ContextVar[ControlContext] = contextvars.ContextVar(
            "control_context")
        self.control_context.set(ControlContext(65536))

    def setup(self):
        n0 = self.create_node("node0", "127.0.0.1", port=12345)
        n1 = self.create_node("node1", "127.0.0.1", port=12346)
        n0.connect(n1, bi=True)

    def setup_local(self):
        n0 = self.create_node("node0", "local", port=12345)
        n1 = self.create_node("node1", "local", port=12346)
        n0.connect(n1, bi=True)

    def create_object(self, node_name: str, module,  *args, **kwargs) -> 'IrisObject':
        node = self.client_wrapper[node_name]
        return node.create_object(module,  *args, **kwargs)

    def create_node(self, name, ip="127.0.0.1", port=None) -> 'IrisNode':
        if ip == "local":
            node = IrisNode(name, ip, port, None, self)
            self.client_wrapper[name] = node
            return node
        port = port if port else self.ports
        self.ports += 1
        stub = self.inner.connect(
            f"/tmp/iris-tmp-node-{ip}-{port}.sock", f"node{ip}:{port}")
        # stub = IrisClientWrapper(, name, self)
        node = IrisNode(name, ip, port, stub, self)
        self.client_wrapper[name] = node
        self.client_wrapper[name].stub.init(
            modules=list(sys.modules.keys()), path=sys.path, rank=len(self.client_wrapper)-1
        )
        return node
    
    def close(self):
        for client in self.client_wrapper.values():
            client.close()

    def __del__(self):
        self.close()

class IrisNode:
    def __init__(self, name: str, ip: str, port: int, stub, ctx: 'IrisContext'):
        self.name: str = name
        self.ip: str = ip
        self.port: int = port
        self.stub = stub
        self.internal_name: str = f"node{ip}:{port}" if ip != "local" else f"{name}_local"
        self.ctx = ctx

    def connect(self, node1: 'IrisNode', bi: bool = False):
        if self.ip == "local":
            return
        self.stub.connect_nodes({
            node1.internal_name: f"{node1.ip}:{node1.port}"
        })
        if bi:
            node1.connect(self, bi=False)

    def __repr__(self) -> str:
        return f"{self.name}#{self.internal_name}"

    def create_object_by_name(self, module_name, qual_name,  *args, **kwargs) -> 'IrisObject':
        if self.ip == "local":
            try:
                m = getattr(globals()[module_name], qual_name)
            except:
                m = getattr(eval(module_name), qual_name)
            r_args, holds_ref = retrieve_args(self, self.ctx, args)
            obj = m(*r_args, **kwargs)
            return IrisObject(obj, self, self.ctx, args, kwargs, i_stack=3)
        r_args, holds_ref = retrieve_args(self, self.ctx, args)
        r = self.stub.create_object(
            module=module_name,
            qualname=qual_name,
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            go_async=self.ctx.config.go_async,
            after_list=self.ctx.control_context.get().get_last_task()
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self, self.ctx, args, kwargs, i_stack=3)

    def create_object(self, module,  *args, **kwargs) -> 'IrisObject':
        if self.ip == "local":
            r_args, holds_ref = retrieve_args(self, self.ctx, args)
            obj = module(*r_args, **kwargs)
            return IrisObject(obj, self, self.ctx, args, kwargs, i_stack=3)
        r_args, holds_ref = retrieve_args(self, self.ctx, args)
        r = self.stub.create_object(
            module=module.__module__,
            qualname=module.__qualname__,
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            go_async=self.ctx.config.go_async,
            after_list=self.ctx.control_context.get().get_last_task()
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self, self.ctx, args, kwargs, i_stack=3)

    def apply(self, func, args, kwargs) -> 'IrisObject':
        if self.ip == "local":
            r_args, holds_ref = retrieve_args(self, self.ctx, args)
            obj = func(*r_args, **kwargs)
            return IrisObject(obj, self, self.ctx, args, kwargs, i_stack=3)
        r_args, holds_ref = retrieve_args(self, self.ctx, args)
        r = self.stub.apply(
            func=dill.dumps(func),
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            go_async=self.ctx.config.go_async,
            after_list=self.ctx.control_context.get().get_last_task()
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self, self.ctx, args, kwargs, i_stack=2)

    def get_remote_object(self, obj: 'IrisObject') -> 'IrisObject':
        if self.ip == "local":
            return obj
        r = self.stub.get_remote_object(obj.inner, go_async=self.ctx.config.go_async,
                                        after_list=self.ctx.control_context.get().get_last_task())
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self, self.ctx, None, None, i_stack=2)

    def send(self, obj) -> 'IrisObject':
        if self.ip == "local":
            return IrisObject(obj, self, self.ctx, None, None, i_stack=3)
        bytes = dill.dumps(obj)
        r = self.stub.send(bytes, go_async=self.ctx.config.go_async,
                                                    after_list=self.ctx.control_context.get().get_last_task())
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self, self.ctx, None, None, i_stack=2)

    def close(self):
        if self.stub:
            self.stub.close()


class IrisObject:
    def __init__(self, inner, node: 'IrisNode', ctx: IrisContext, args, kwargs, attrs=[], i_stack=1):
        super().__init__()
        if isinstance(inner, IrisObject):
            raise NotImplementedError()
        if isinstance(inner, IrisObjectInternal):
            self.inner: IrisObjectInternal = inner
            self.value: Any = inner.get_native_value()
            self.type = inner.get_type()
            self.id: IrisObjectId = inner.id()
        else:
            self.value: Any = inner
            self.type = str(type(inner))
        self.node: 'IrisNode' = node
        self.ctx: IrisContext = ctx
        self.args = args
        self.kwargs = kwargs
        self.attrs: List[str] = attrs
        self.source: Optional[Traceback] = getframeinfo(
            stack()[i_stack][0]) if self.ctx.config.debug else None

    def log(self, msg, i_stack=2):
        if self.ctx.config.debug:
            if self.node.ip == "local":
                print(msg)
            else:
                if i_stack is None:
                    self.inner.log("", 0, msg)
                else:
                    source = getframeinfo(stack()[i_stack][0])
                    self.inner.log(source.filename, source.lineno, msg)

    def __repr__(self):
        if self.node.ip == "local":
            return self.value.__repr__()
        if self.ctx.config.debug:
            return f"Remote Object #{self.inner.id().id} on {self.node}, Type {self.type}, at {self.source.filename}:{self.source.lineno}"
        else:
            return f"Remote Object #{self.inner.id().id} on {self.node}, Type {self.type}"

    """
    Get a copy of object in remote node
    """

    def get(self):
        if self.value is not None:
            return self.value
        data = self.inner.get_value(self.attrs)
        return dill.loads(data)

    def __call__(self, *args, **kwargs) -> 'IrisObject':
        if self.value is not None:
            if self.node.ip == "local":
                r_args, holds_ref = retrieve_args(self.node, self.ctx, args)
                obj = self.value(*r_args, **kwargs)
                return IrisObject(obj, self.node, self.ctx, args, kwargs, i_stack=2)
            raise NotImplementedError()
        r_args, holds_ref = retrieve_args(self.node, self.ctx, args)
        self.log("call")
        r = self.inner.call(
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            attr=self.attrs,
            go_async=self.ctx.config.go_async,
            after_list=self.ctx.control_context.get().get_last_task()
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        self.log(f"CALL => {r.id().id}", i_stack=2)
        return IrisObject(r, self.node, self.ctx, args, kwargs, i_stack=2)

    def _call_with_attr(self, attr: str, go_async: bool, args=[], kwargs={}, i_stack=3) -> 'IrisObject':
        if self.value is not None:
            if self.node.ip == "local":
                r_args, holds_ref = retrieve_args(self.node, self.ctx, args)
                obj = getattr(self.value, attr)(*r_args, **kwargs)
                return IrisObject(obj, self.node, self.ctx, args, kwargs, i_stack=i_stack)
            if args == None:
                return getattr(self.value, attr)()
            return getattr(self.value, attr)(*args)
        r_args, holds_ref = retrieve_args(self.node, self.ctx, args)
        r = self.inner.call(
            b_args=r_args,
            b_kwargs=kwargs,
            attr=[*self.attrs, attr], pickle=dill,
            go_async=go_async,
            after_list=self.ctx.control_context.get().get_last_task()
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        self.log(f"CALLATTR {attr} => {r.id().id}", i_stack=i_stack)
        return IrisObject(r, self.node, self.ctx, args, kwargs, i_stack=i_stack)

    def keys(self):
        return self._call_with_attr('keys', go_async=self.ctx.config.go_async, args=None)

    def to_node(self, node: 'IrisNode'):
        if self.node.ip == "local":
            if isinstance(self.value, torch.Tensor):
                return IrisObject(copy.deepcopy(self.value.detach()), node, self.ctx, None, None, i_stack=2)
            else:
                return IrisObject(copy.deepcopy(self.value), node, self.ctx, None, None, i_stack=2)
        if node.name == self.node.name:
            return self
        return node.get_remote_object(self)

    def __getattr__(self, attr):
        if self.node.ip == "local":
            return IrisObject(getattr(self.value, attr), self.node, self.ctx, None, None, i_stack=2)
        # TODO: add options
        if True:
            r = self.inner.get_attr([attr], go_async=self.ctx.config.go_async,
                                    after_list=self.ctx.control_context.get().get_last_task())
            if r.exception():
                exception = dill.loads(r.exception())
                raise exception
            if self.ctx.config.go_async_sequence:
                self.ctx.control_context.get().set_last_task(r.id())
            self.log(f"GETATTR {attr} => {r.id().id}", i_stack=2)
            return IrisObject(r, self.node, self.ctx, None, None, i_stack=2)
        else:
            return IrisObject(self.inner.clone(), self.node, self.ctx, None, None, [*self.attrs, attr], i_stack=2)

    def __add__(self, other):
        return self._call_with_attr('__add__', go_async=self.ctx.config.go_async, args=(other,))

    def __sub__(self, other):
        return self._call_with_attr('__sub__', go_async=self.ctx.config.go_async, args=(other,))

    def __mul__(self, other):
        return self._call_with_attr('__mul__', go_async=self.ctx.config.go_async, args=(other,))

    def __div__(self, other):
        return self._call_with_attr('__div__', go_async=self.ctx.config.go_async, args=(other,))
    # TODO: Add more magic methods

    def __len__(self):
        return self._call_with_attr('__len__', go_async=self.ctx.config.go_async, args=())

    def __iter__(self):
        return self._call_with_attr('__iter__', go_async=self.ctx.config.go_async, args=())
        # return AsyncIterator(self._call_with_attr('__iter__',go_async=self.ctx.config.go_async, args=None))

    def __getitem__(self, key):
        return self._call_with_attr('__getitem__', go_async=self.ctx.config.go_async, args=(key,))

    def __setitem__(self, key, value):
        return self._call_with_attr('__setitem__', go_async=self.ctx.config.go_async, args=(key, value,))

    def __delitem__(self, key):
        return self._call_with_attr('__delitem__', go_async=self.ctx.config.go_async, args=(key,))

    def __reversed__(self):
        return self._call_with_attr('__reversed__', go_async=self.ctx.config.go_async, args=())

    def __contains__(self, item):
        return self._call_with_attr('__contains__', go_async=self.ctx.config.go_async, args=(item,))

    def __next__(self):
        return self._call_with_attr('__next__', go_async=False, args=())

    def __index__(self):
        if self.value is not None:
            return getattr(self.value, '__index__')()
        return self._call_with_attr('__index__', go_async=self.ctx.config.go_async, args=()).get()

    def __del__(self):
        if self.node.ip == "local":
            return
        self.log("del", i_stack=None)
        self.inner.del_obj(go_async=self.ctx.config.go_async,
                           after_list=self.ctx.control_context.get().get_last_task())

class IrisObjectWrapper:
    def __init__(self, object) -> None:
        super().__init__()
        self.object = object
        self.node = object.node

    def get(self):
        return self.object.get()

class AsyncIterator:
    def __init__(self, inner):
        self.inner = inner
        self.current = 0
        try:
            self.len = inner._call_with_attr(
                '__len__', go_async=False, args=None).value
        except AttributeError:
            self.len = None

    def __len__(self):
        return self.len

    def __next__(self):
        if self.len is None:
            return self.inner._call_with_attr('__next__', go_async=self.inner.ctx.config.go_async, args=None)
        else:
            self.current += 1
            if self.current >= self.len:
                return self.inner._call_with_attr('__next__', go_async=False, args=None)
            else:
                return self.inner._call_with_attr('__next__', go_async=self.inner.ctx.config.go_async, args=None)


class RemoteTensorGroup:
    def __init__(self):
        super().__init__()
        self.inputs = []
        self.outputs: dict[Any, Any] = {}

    def add_input(self, source, this):
        source.incr_output()
        self.inputs.append((this, source))

    def add_output(self, tensor: 'IrisObject'):
        if tensor.node.ip == "local":
            object_id = id(tensor.value)
        else:
            object_id = tensor.id.id
        if object_id in self.outputs:
            self.outputs[object_id][0] += 1
        else:
            self.outputs[object_id] = [0, tensor]

    def backward(self, tensor, grad):
        if isinstance(tensor, IrisObjectWrapper):
            tensor = tensor.object
        if tensor.node.ip == "local":
            object_id = id(tensor.value)
        else:
            object_id = tensor.id.id
        if self.outputs[object_id][0] == 1:
            self.outputs[object_id][1].backward(grad)
        else:
            self.outputs[object_id][1].backward(grad, retain_graph=True)
            self.outputs[object_id][0] -= 1
        for input_this, input_source in self.inputs:
            input_source.backward(input_this.grad)


class RemoteTensor(IrisObjectWrapper):
    def __init__(self, inner: IrisObject, group=None):
        super(RemoteTensor, self).__init__(inner)
        self.group = group if group else RemoteTensorGroup()
        self.parents = [p[1] for p in self.group.inputs]

    def incr_output(self):
        self.group.add_output(self.object)

    def backward(self, grad=None):
        # print(f"backward on {self.object}")
        # if grad is not None:
        #     print(grad.get())
        # else:
        #     print("none")
        # print("none" if not grad else grad.get())
        self.group.backward(self, grad)

    def sum(self):
        ret = self.object.sum()
        self.group.add_output(ret)
        ret = RemoteTensor(ret, self.group)
        return ret


class IrisModel:
    def __init__(self, model):
        super().__init__()
        self.model = model

    def forward(self, *args):
        # if self.model.node.ip == "local":
        #     return self.model(*args)
        r_args = []
        input_pair = []
        group = RemoteTensorGroup()
        for a in args:
            if isinstance(a, RemoteTensor):
                if a.object.node != self.model.node:
                    this = a.object.to_node(self.model.node)
                    if a.object.node.ip == "local":
                        this.requires_grad_(True)
                        a.object.detach()
                    group.add_input(a, this)
                    r_args.append(this)
                else:
                    r_args.append(a)
            else:
                r_args.append(a)
        ret = self.model(*r_args)
        group.add_output(ret)
        ret = RemoteTensor(ret, group)
        return ret

    def __call__(self, *args):
        return self.forward(*args)


def retrieve_args(node, ctx, args, cls=tuple):
    if args is None:
        return None, None
    a = []
    holds_ref = []
    for arg in args:
        if isinstance(arg, IrisObjectWrapper):
            if arg.node.ip == "local":
                if arg.node.name == node.name:
                    a.append(arg.object.value)
                else:
                    if isinstance(arg.object.value, torch.Tensor):
                        a.append(copy.deepcopy(arg.object.value.detach()))
                    else:
                        a.append(copy.deepcopy(arg.object.value))
            else:
                x = arg.object.id.add_attr(arg.object.attrs)
                a.append(x)
        elif isinstance(arg, IrisObject):
            if arg.node.ip == "local":
                if arg.node.name == node.name:
                    a.append(arg.value)
                else:
                    if isinstance(arg.value, torch.Tensor):
                        a.append(copy.deepcopy(arg.value.detach()))
                    else:
                        a.append(copy.deepcopy(arg.value))
            else:
                x = arg.id.add_attr(arg.attrs)
                a.append(x)
        elif isinstance(arg, list):
            rr = retrieve_args(node, ctx, a,  list)
            holds_ref.extend(rr[1])
            a.append(rr[0])
        elif isinstance(arg, dict):
            a.append(arg)
            # raise NotImplementedError(arg)
        elif isinstance(arg, tuple):
            rr = retrieve_args(node, ctx,  a)
            holds_ref.extend(rr[1])
            a.append(rr[0])
        else:
            a.append(arg)
    return cls(a), holds_ref


class RemoteFunction:
    def __init__(self, func):
        self.func = func
        self.func_bytes = dill.dumps(self.func)
        self.cache_objects: dict['IrisNode', 'IrisObject'] = {}

    def to_node(self, node: 'IrisNode'):
        ctx = node.ctx
        if node.ip == "local":
            self.cache_objects[node] = IrisObject(self.func, node, ctx, None, None, i_stack=2)
            return
        r = ctx.client_wrapper[node.name].stub.send(self.func_bytes, go_async=ctx.config.go_async,
                                                    after_list=ctx.control_context.get().get_last_task())
        if ctx.config.go_async_sequence:
            ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        self.cache_objects[node] = IrisObject(
            r, node, ctx, None, None, i_stack=2)

    def on(self, node: 'IrisNode'):
        if node not in self.cache_objects:
            self.to_node(node)
        return self.cache_objects[node]


class _RTF:
    def __init__(self, func_object: IrisObject, node: IrisNode) -> None:
        super().__init__()
        self.func_object: IrisObject = func_object
        self.node: IrisNode = node

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        group = None
        for a in args:
            if isinstance(a, RemoteTensor) and a.object.node.name == self.node.name:
                group = a.group
                break
        ret = self.func_object(*args, **kwds)
        if not isinstance(ret, IrisObject):
            raise NotImplementedError()
        if group is None:
            return ret
        group.add_output(ret)
        ret = RemoteTensor(ret, group)
        return ret


class RemoteTensorFunction:
    def __init__(self, func):
        self.func = func
        self.func_bytes = dill.dumps(self.func)
        self.cache_objects: dict['IrisNode', '_RTF'] = {}

    def to_node(self, node: 'IrisNode'):
        ctx = node.ctx
        if node.ip == "local":
            self.cache_objects[node] = _RTF(IrisObject(self.func, node, ctx, None, None, i_stack=2), node)
            return
        r = ctx.client_wrapper[node.name].stub.send(self.func_bytes, go_async=ctx.config.go_async,
                                                    after_list=ctx.control_context.get().get_last_task())
        if ctx.config.go_async_sequence:
            ctx.control_context.get().set_last_task(r.id())
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        self.cache_objects[node] = _RTF(
            IrisObject(r, node, ctx, None, None, i_stack=2), 
            node
        )

    def on(self, node: 'IrisNode'):
        if node not in self.cache_objects:
            self.to_node(node)
        return self.cache_objects[node]


def remote():
    def wrapper(func):
        return RemoteFunction(func)
    return wrapper
