from .client import IrisContextInternal, IrisObjectId
import dill
import sys
import dill
import functools
import torch.distributed.autograd as dist_autograd
from torch import optim
from torch.distributed.optim import DistributedOptimizer
# from proto.helloworld.helloworld_pb2 import *
import asyncio
import traceback

# class ObjectId:
#     def __init__(self, id):
#         super().__init__()
#         self.id = id

#     def __int__(self):
#         return self.id


class IrisContext:
    def __init__(self):
        super().__init__()
        self.inner = IrisContextInternal()
        self.client_wrapper = {}

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
            "node127.0.0.1:12346": "http://127.0.0.1:12346"
        })
        self.client_wrapper["node1"].inner.connect_nodes({
            "node127.0.0.1:12345": "http://127.0.0.1:12345"
        })

    def create_object(self, node, module,  *args, **kwargs):
        inner_client = self.client_wrapper[node]
        return inner_client.create_object(module,  *args, **kwargs)


class IrisGradContext:
    def __init__(self, ctx, node):
        super().__init__()
        self.ctx = ctx
        self.node = node

    def __enter__(self):
        self.torch_autograd_context = self.ctx.client_wrapper[self.node].create_object(
            dist_autograd.context, False)
        self.context_id = self.torch_autograd_context.__enter__()
        return self.context_id

    def __exit__(self, type, value, track):
        self.torch_autograd_context.__exit__(type, value, track)


class IrisOptimizer:
    def __init__(self, ctx, node, models, context_id):
        super().__init__()
        self.node = node
        self.models = models
        self.ctx = ctx
        self.context_id = context_id

        self.model_parameters = [self.get_parameter(m) for m in self.models]
        self.model_parameters = self.ctx.client_wrapper[node].batch_wait(
            self.model_parameters)
        self.model_parameters_id = [IrisObjectId(
            m.id()) for m in self.model_parameters]

        parameters = self.ctx.client_wrapper[node].apply(
            func=lambda *x: functools.reduce(lambda a, b: a+b, x),
            args=(self.model_parameters_id),
            kwargs=None,
            pickle=dill
        )
        # b = optim.SGD
        b = optim.Adadelta
        self.optimizer = self.ctx.client_wrapper[node].create_object(
            DistributedOptimizer,
            False,
            ModuleRef(module=b.__module__, qualname=b.__qualname__),
            parameters,
            lr=1.0
        )

    def get_loss(self, loss):
        if loss.node == self.node:
            return loss
        r_a = self.ctx.client_wrapper[self.node].inner.torch_call(
            target_node=loss.node,
            object_id=loss.id.id,
            torch_func="torch_GetObject",
            to_here=True,
            pickle=dill
        )
        return IrisObject(r_a, self.node, self.ctx, None, None)

    def backward(self, loss):
        if type(loss) is list:
            loss = list(map(self.get_loss, loss))
            loss = list(map(lambda x: x.id, loss))
        else:
            loss = self.get_loss(loss)
            loss = [loss.id]
        args = (self.context_id.id, loss)
        module = dist_autograd.backward
        self.ctx.client_wrapper[self.node].inner.create_object(
            module=module.__module__,
            qualname=module.__name__,
            b_args=args,
            b_kwargs=None,
            pickle=dill
        )

    def step(self):
        self.optimizer.step(self.context_id)

    def get_parameter(self, model):
        if model.node == self.node:
            return self.ctx.client_wrapper[self.node].get_parameter_async(model.id)
        else:
            return self.ctx.client_wrapper[self.node].inner.torch_call_async(
                target_node=model.node,
                object_id=model.id.id,
                torch_func="torch_GetParameters",
                to_here=True,
                pickle=dill
            )


class IrisObject:
    def __init__(self, inner, node, ctx, args, kwargs):
        super().__init__()
        self.inner = inner
        self.node = node
        self.ctx = ctx
        self.id = inner.id()
        self.value = inner.get_native_value()
        self.type = inner.get_type()
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return f"Remote Object #{self.inner.id().id} on {self.node}, Type {self.type}"

    """
    Get a copy of object in remote node
    """

    def get(self):
        if self.value:
            return self.value
        data = self.inner.get_value()
        return dill.loads(data)

    def __call__(self, *args, **kwargs):
        if self.value:
            raise NotImplementedError()
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.call(
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill
        )
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs)

    def _call_with_attr(self, attr, args, kwargs={}):
        if self.value:
            if args == None:
                return getattr(self.value, attr)()
            return getattr(self.value, attr)(*args)
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.call(
            b_args=r_args,
            b_kwargs=kwargs,
            attr=attr, pickle=dill
        )
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs)

    def keys(self):
        return self._call_with_attr('keys', args=None)

    def to_node(self, node):
        return self.ctx.client_wrapper[node].get_remote_object(self)

    def __getattr__(self, attr):
        r = self.inner.get_attr(attr)
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, None, None)

    def __add__(self, other):
        return self._call_with_attr('__add__', args=(other,))

    def __sub__(self, other):
        return self._call_with_attr('__sub__', args=(other,))

    def __mul__(self, other):
        return self._call_with_attr('__mul__', args=(other,))

    def __div__(self, other):
        return self._call_with_attr('__div__', args=(other,))
    # TODO: Add more magic methods

    def __len__(self):
        return self._call_with_attr('__len__', args=None)

    def __iter__(self):
        return self._call_with_attr('__iter__', args=None)

    def __getitem__(self, key):
        return self._call_with_attr('__getitem__', args=(key,))

    def __setitem__(self, key, value):
        return self._call_with_attr('__setitem__', args=(key, value,))

    def __delitem__(self, key):
        return self._call_with_attr('__delitem__', args=(key,))

    def __reversed__(self):
        return self._call_with_attr('__reversed__', args=None)

    def __contains__(self, item):
        return self._call_with_attr('__contains__', args=(item,))

    def __next__(self):
        return self._call_with_attr('__next__', args=None)

    def __index__(self):
        if self.value:
            return getattr(self.value, '__index__')()
        return self._call_with_attr('__index__', args=None).get()


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

    def batch_wait(self, tasks):
        return self.inner.batch_wait([m.inner for m in tasks])

    def create_object(self, module,  *args, **kwargs):
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.create_object(
            module=module.__module__,
            qualname=module.__qualname__,
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill
        )
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        # print("time cost", r.time_cost_as_sec())
        return IrisObject(r, self.node, self.ctx, args, kwargs)

    def torch_call(self):
        pass

    def get_parameter(self, object_id):
        r = self.inner.get_parameter(object_id.id)
        return IrisObject(r, self.node, self.ctx, None, None)

    def get_parameter_async(self, object_id):
        return IrisAsyncTask(self.inner.get_parameter_async(object_id.id), self.inner)

    def apply(self, func, args, kwargs):
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.apply(
            func=dill.dumps(func),
            b_args=r_args,
            b_kwargs=kwargs,
            pickle=dill
        )
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs)

    def get_remote_object(self, obj):
        r = self.inner.get_remote_object(obj.inner)
        return IrisObject(r, self.node, self.ctx, None, None)


def retrieve_args(self, node, ctx, args, cls=tuple):
    if args is None:
        return None, None
    a = []
    holds_ref = []
    for arg in args:
        if type(arg) is IrisObject:
            a.append(arg.id)
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
