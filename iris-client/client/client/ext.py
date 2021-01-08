from .client import IrisContextInternal, IrisObjectId
import dill
import sys
import functools
from torch import optim
import asyncio
from inspect import getframeinfo, stack
import functools

class IrisConfig:
    def __init__(self):
        self.go_async = True
        self.go_async_sequence = True
        self.debug = True
        self.log_color = True

class IrisContext:
    def __init__(self, config = None):
        super().__init__()
        self.inner = IrisContextInternal(config)
        self.client_wrapper = {}
        self.config = config if config else IrisConfig()
        self.last_task = None

    def setup(self):
        self.client_wrapper["node0"] = IrisClientWrapper(self.inner.connect(
            "/tmp/iris-tmp-node-127.0.0.1-12345.sock", f"node127.0.0.1:12345"), "node0", self)
        self.client_wrapper["node1"] = IrisClientWrapper(self.inner.connect(
            "/tmp/iris-tmp-node-127.0.0.1-12346.sock", f"node127.0.0.1:12346"), "node1", self)
        # self.client_wrapper["node2"] = IrisClientWrapper(self.inner.connect("/tmp/iris-tmp-node2-2.sock"), "node2", self)
        self.client_wrapper["node0"].inner.init(
            modules=list(sys.modules.keys()), path=sys.path, rank=0)
        self.client_wrapper["node1"].inner.init(
            modules=list(sys.modules.keys()), path=sys.path, rank=1)
        # self.client_wrapper["node2"].inner.init(modules = list(sys.modules.keys()), path=sys.path, rank=2)
        self.client_wrapper["node0"].inner.connect_nodes({
            "node127.0.0.1:12346": "127.0.0.1:12346"
        })
        self.client_wrapper["node1"].inner.connect_nodes({
            "node127.0.0.1:12345": "127.0.0.1:12345"
        })

    def create_object(self, node, module,  *args, **kwargs):
        inner_client = self.client_wrapper[node]
        return inner_client.create_object(module,  *args, **kwargs)

class IrisObject:
    def __init__(self, inner, node, ctx, args, kwargs, attrs=[], i_stack=1):
        super().__init__()
        self.inner = inner
        self.node = node
        self.ctx = ctx
        self.id = inner.id()
        self.value = inner.get_native_value()
        self.type = inner.get_type()
        self.args = args
        self.kwargs = kwargs
        self.attrs = attrs
        self.source = getframeinfo(stack()[i_stack][0]) if self.ctx.config.debug else None
        self.log("create", i_stack=i_stack+1)

    def log(self, msg, i_stack=2):
        if self.ctx.config.debug:
            if i_stack is None:
                self.inner.log("",0,msg)
            else:
                source = getframeinfo(stack()[i_stack][0])
                self.inner.log(source.filename, source.lineno, msg)

    def __repr__(self):
        if self.ctx.config.debug:
            return f"Remote Object #{self.inner.id().id} on {self.node}, Type {self.type}, at {self.source.filename}:{self.source.lineno}"
        else:
            return f"Remote Object #{self.inner.id().id} on {self.node}, Type {self.type}"

    """
    Get a copy of object in remote node
    """

    def get(self):
        if self.value:
            return self.value
        data = self.inner.get_value(self.attrs)
        return dill.loads(data)

    def __call__(self, *args, **kwargs):
        if self.value:
            raise NotImplementedError()
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        self.log("call")
        r = self.inner.call(
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            attr=self.attrs,
            go_async=self.ctx.config.go_async,
            after_list = self.ctx.last_task
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.last_task = [r.id()]
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs, i_stack=2)

    def _call_with_attr(self, attr, go_async, args, kwargs={}, i_stack=3):
        if self.value:
            if args == None:
                return getattr(self.value, attr)()
            return getattr(self.value, attr)(*args)
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        self.log("call with attr", i_stack)
        r = self.inner.call(
            b_args=r_args,
            b_kwargs=kwargs,
            attr=[*self.attrs,attr], pickle=dill,
            go_async=go_async,
            after_list = self.ctx.last_task
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.last_task = [r.id()]
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs, i_stack=i_stack)

    def keys(self):
        return self._call_with_attr('keys', go_async=self.ctx.config.go_async, args=None)

    def to_node(self, node):
        if node == self.node:
            return self
        return self.ctx.client_wrapper[node].get_remote_object(self)

    def __getattr__(self, attr):
        # TODO: add options
        if True:
            r = self.inner.get_attr([attr],go_async=self.ctx.config.go_async,after_list = self.ctx.last_task)
            if r.exception():
                exception = dill.loads(r.exception())
                raise exception
            if self.ctx.config.go_async_sequence:
                self.ctx.last_task = [r.id()]
            return IrisObject(r, self.node, self.ctx, None, None, i_stack=2)
        else:
            return IrisObject(self.inner.clone(), self.node, self.ctx, None, None, [*self.attrs, attr], i_stack=2)

    def __add__(self, other):
        return self._call_with_attr('__add__',go_async=self.ctx.config.go_async, args=(other,))

    def __sub__(self, other):
        return self._call_with_attr('__sub__',go_async=self.ctx.config.go_async, args=(other,))

    def __mul__(self, other):
        return self._call_with_attr('__mul__',go_async=self.ctx.config.go_async, args=(other,))

    def __div__(self, other):
        return self._call_with_attr('__div__',go_async=self.ctx.config.go_async, args=(other,))
    # TODO: Add more magic methods

    def __len__(self):
        return self._call_with_attr('__len__',go_async=self.ctx.config.go_async, args=None)

    def __iter__(self):
        return self._call_with_attr('__iter__',go_async=self.ctx.config.go_async, args=None)
        # return AsyncIterator(self._call_with_attr('__iter__',go_async=self.ctx.config.go_async, args=None))

    def __getitem__(self, key):
        return self._call_with_attr('__getitem__',go_async=self.ctx.config.go_async, args=(key,))

    def __setitem__(self, key, value):
        return self._call_with_attr('__setitem__',go_async=self.ctx.config.go_async, args=(key, value,))

    def __delitem__(self, key):
        return self._call_with_attr('__delitem__',go_async=self.ctx.config.go_async, args=(key,))

    def __reversed__(self):
        return self._call_with_attr('__reversed__',go_async=self.ctx.config.go_async, args=None)

    def __contains__(self, item):
        return self._call_with_attr('__contains__',go_async=self.ctx.config.go_async, args=(item,))

    def __next__(self):
        return self._call_with_attr('__next__',go_async=False, args=None)

    def __index__(self):
        if self.value:
            return getattr(self.value, '__index__')()
        return self._call_with_attr('__index__',go_async=self.ctx.config.go_async, args=None).get()

    def __del__(self):
        self.log("del",i_stack=None)
        self.inner.del_obj(go_async=self.ctx.config.go_async,after_list = self.ctx.last_task)

class AsyncIterator:
    def __init__(self, inner):
        self.inner = inner
        self.current = 0
        try:
            self.len = inner._call_with_attr('__len__',go_async=False, args=None).value
        except AttributeError:
            self.len = None
    
    def __len__(self):
        return self.len

    def __next__(self):
        if self.len is None:
            return self.inner._call_with_attr('__next__',go_async=self.inner.ctx.config.go_async, args=None)
        else:
            self.current += 1
            if self.current >= self.len:
                return self.inner._call_with_attr('__next__',go_async=False, args=None)
            else:
                return self.inner._call_with_attr('__next__',go_async=self.inner.ctx.config.go_async, args=None)


class RemoteTensorGroup:
    def __init__(self):
        super().__init__()
        self.inputs = []
        self.outputs = {}

    def add_input(self, source, this):
        source.incr_output()
        self.inputs.append((this, source))

    def add_output(self, tensor):
        object_id = tensor.id.id
        if object_id in self.outputs:
            self.outputs[object_id][0] += 1
        else:
            self.outputs[object_id] = [0, tensor]

    def backward(self, tensor, grad):
        object_id = tensor.id.id
        if self.outputs[object_id][0] == 1:
            self.outputs[object_id][1].backward(grad)
        else:
            self.outputs[object_id][1].backward(grad, retain_graph=True)
            self.outputs[object_id][0] -= 1
        for input_this, input_source in self.inputs:
            input_source.backward(input_this.grad)


class RemoteTensor:
    def __init__(self, inner, group=None):
        super().__init__()
        self.inner = inner
        self.group = group if group else RemoteTensorGroup()
        self.parents = [p[1] for p in self.group.inputs]

    def incr_output(self):
        self.group.add_output(self.inner)

    def backward(self, grad=None):
        # if grad is not None:
        #     print(grad.get())
        # else:
        #     print("none")
        # print("none" if not grad else grad.get())
        self.group.backward(self.inner, grad)

    def sum(self):
        ret = self.inner.sum()
        self.group.add_output(ret)
        return RemoteTensor(ret, self.group)


class IrisModel:
    def __init__(self, model):
        super().__init__()
        self.model = model

    def forward(self, *args):
        r_args = []
        input_pair = []
        group = RemoteTensorGroup()
        for a in args:
            if type(a) is RemoteTensor:
                if a.inner.node != self.model.node:
                    this = a.inner.to_node(self.model.node)
                    group.add_input(a, this)
                    r_args.append(this)
                else:
                    r_args.append(a.inner)
            else:
                r_args.append(a)
        ret = self.model(*r_args)
        group.add_output(ret)
        ret = RemoteTensor(ret, group)
        return ret

    def __call__(self, *args):
        return self.forward(*args)


class IrisAsyncTask:
    def __init__(self, inner, client):
        super().__init__()
        self.loop = asyncio.get_running_loop()
        self.inner = inner
        self.client = client

    def to_fut(self):
        fut = self.loop.create_future()
        loop = self.loop

        def resolve_future(result):
            async def resolve(r):
                fut.set_result(r)
            asyncio.run_coroutine_threadsafe(resolve(result), loop)

        self.client.add_set_result(self.inner, resolve_future)
        # self.client.add_set_exception(inner, fut.set_exception)
        return fut


class IrisClientWrapper:
    def __init__(self, inner, node, ctx):
        super().__init__()
        self.inner = inner
        self.node = node
        self.ctx = ctx
        self.attrs = []

    def batch_wait(self, tasks):
        return self.inner.batch_wait([m.inner for m in tasks])

    def create_object(self, module,  *args, **kwargs):
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.create_object(
            module=module.__module__,
            qualname=module.__qualname__,
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            go_async=self.ctx.config.go_async,
            after_list = self.ctx.last_task
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.last_task = [r.id()]
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        # print("time cost", r.time_cost_as_sec())
        return IrisObject(r, self.node, self.ctx, args, kwargs, i_stack=3)

    def apply(self, func, args, kwargs):
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.apply(
            func=dill.dumps(func),
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill,
            go_async=self.ctx.config.go_async,
            after_list = self.ctx.last_task
        )
        if self.ctx.config.go_async_sequence:
            self.ctx.last_task = [r.id()]
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs,i_stack=2)

    def get_remote_object(self, obj):
        r = self.inner.get_remote_object(obj.inner,go_async=self.ctx.config.go_async,
            after_list = self.ctx.last_task)
        if self.ctx.config.go_async_sequence:
            self.ctx.last_task = [r.id()]
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, None, None, i_stack=2)



def retrieve_args(self, node, ctx, args, cls=tuple):
    if args is None:
        return None, None
    a = []
    holds_ref = []
    for arg in args:
        if type(arg) is IrisObject:
            x = arg.id.add_attr(arg.attrs)
            a.append(x)
        elif type(arg) is list:
            rr = retrieve_args(self, node, ctx, a,  list)
            holds_ref.extend(rr[1])
            a.append(rr[0])
        elif type(arg) is dict:
            raise NotImplementedError()
        elif type(arg) is tuple:
            rr = retrieve_args(self, node, ctx,  a)
            holds_ref.extend(rr[1])
            a.append(rr[0])
        else:
            a.append(arg)
    return cls(a), holds_ref

class RemoteFunction:
    def __init__(self, func, ctx):
        self.func = func
        self.ctx = ctx
        self.func_bytes = dill.dumps(self.func)
        self.cache_objects = {}

    def to_node(self, node):
        r = self.ctx.client_wrapper[node].inner.send(self.func_bytes, go_async=self.ctx.config.go_async,
            after_list = self.ctx.last_task)
        if self.ctx.config.go_async_sequence:
            self.ctx.last_task = [r.id()]
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        self.cache_objects[node] = IrisObject(r, node, self.ctx, None, None, i_stack=2)

    def on(self, node):
        if node not in self.ctx.client_wrapper:
            raise NotImplementedError()
        if node not in self.cache_objects:
            self.to_node(node)
        return self.cache_objects[node]

def remote(ctx):
    if ctx is None:
        raise NotImplementedError()
    def wrapper(func):
        return RemoteFunction(func, ctx)
    return wrapper