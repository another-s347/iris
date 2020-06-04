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
        self.client_wrapper["node0"] = IrisClientWrapper(self.inner.connect("/tmp/iris-tmp-node0-0.sock"), "node0", self)
        # self.client_wrapper["node1"] = IrisClientWrapper(self.inner.connect("/tmp/iris-tmp-node1-1.sock"), "node1", self)
        # self.client_wrapper["node2"] = IrisClientWrapper(self.inner.connect("/tmp/iris-tmp-node2-2.sock"), "node2", self)
        self.client_wrapper["node0"].inner.init(modules = list(sys.modules.keys()), path=sys.path, rank=0)
        # self.client_wrapper["node1"].inner.init(modules = list(sys.modules.keys()), path=sys.path, rank=1)
        # self.client_wrapper["node2"].inner.init(modules = list(sys.modules.keys()), path=sys.path, rank=2)

    def create_object(self, node, module, recursive, *args, **kwargs):
        inner_client = self.client_wrapper[node]
        return inner_client.create_object(module, recursive, *args, **kwargs)

class IrisGradContext:
    def __init__(self, ctx, node):
        super().__init__()
        self.ctx = ctx
        self.node = node

    def __enter__(self):
        self.torch_autograd_context = self.ctx.client_wrapper[self.node].create_object(dist_autograd.context, False)
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
        self.model_parameters = self.ctx.client_wrapper[node].batch_wait(self.model_parameters)
        self.model_parameters_id = [IrisObjectId(m.id()) for m in self.model_parameters]

        parameters = self.ctx.client_wrapper[node].apply(
            func=lambda *x: functools.reduce(lambda a,b:a+b,x),
            args=(self.model_parameters_id),
            kwargs=None,
            recursive=True,
            pickle=dill
        )
        # b = optim.SGD
        b = optim.Adadelta
        self.optimizer = self.ctx.client_wrapper[node].create_object(
            DistributedOptimizer,
            False,
            ModuleRef(module=b.__module__,qualname=b.__qualname__), 
            parameters,
            lr = 1.0
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
            loss = list(map(lambda x:x.id, loss))
        else:
            loss = self.get_loss(loss)
            loss = [loss.id]
        args = (self.context_id.id, loss)
        module = dist_autograd.backward
        self.ctx.client_wrapper[self.node].inner.create_object(
            module = module.__module__,
            qualname = module.__name__,
            b_args = args,
            b_kwargs = None,
            recursive = True,
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
        self.id = IrisObjectId(inner.id())
        self.value = inner.get_native_value()
        self.type = inner.get_type()
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return f"Remote Object #{self.inner.id()} on {self.node}, Type {self.type}"

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
            recursive=False,
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
            recursive=False,
            attr=attr,pickle=dill
        )
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs)
    
    def keys(self):
        return self._call_with_attr('keys', args=None)

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

class IrisModel:
    def __init__(self, model, ctx, optimizer):
        super().__init__()
        self.model = model
        self.ctx = ctx
        self.optimizer = optimizer
        self.train = True
    
    def forward(self, *args, **kwargs):
        if self.train:
            r_args, holds_ref = retrieve_args(self, self.optimizer.node, self.ctx, args)
            r_a = self.ctx.client_wrapper[self.optimizer.node].inner.torch_call(
                target_node=self.model.node,
                object_id=self.model.id.id,
                to_here=True,
                b_args=r_args,
                b_kwargs=kwargs,
                pickle=dill
            )
            return IrisObject(r_a, self.optimizer.node, self.ctx, args, kwargs)
        else:
            return self.model.forward(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.forward(*args, **kwargs)

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
    
    def create_object(self, module, recursive, *args, **kwargs):
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.create_object(
            module = module.__module__,
            qualname = module.__qualname__,
            b_args = r_args,
            b_kwargs = kwargs,
            recursive = recursive,
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

    def apply(self, func, args, kwargs, recursive):
        r_args, holds_ref = retrieve_args(self, self.node, self.ctx, args)
        r = self.inner.apply(
            func=dill.dumps(func),
            b_args=r_args,
            b_kwargs=kwargs,
            recursive=recursive,
            pickle=dill
        )
        if r.exception():
            exception = dill.loads(r.exception())
            raise exception
        return IrisObject(r, self.node, self.ctx, args, kwargs)
    
def retrieve_args(self, node, ctx, args, recursive=False, cls=tuple):
    if args is None:
        return None, None
    a = []
    holds_ref = []
    for arg in args:
        if type(arg) is IrisObject and arg.node != node:
            r_a = ctx.client_wrapper[node].inner.torch_call(
                target_node=arg.node,
                object_id=arg.id.id,
                torch_func="torch_GetObject",
                to_here=True,
                pickle=dill
            )
            holds_ref.append(r_a)
            a.append(IrisObjectId(r_a.id()))
        elif type(arg) is IrisObject:
            a.append(arg.id)
        elif recursive and type(arg) is list:
            rr = retrieve_args(self, node, ctx, a, recursive, list)
            holds_ref.extend(rr[1])
            a.append(rr[0])
        elif recursive and type(arg) is dict:
            raise NotImplementedError()
        elif recursive and type(arg) is tuple:
            rr = retrieve_args(self, node, ctx, recursive, a)
            holds_ref.extend(rr[1])
            a.append(rr[0])
        else:
            a.append(arg)
    return cls(a), holds_ref
