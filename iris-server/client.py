import argparse
import asyncio
import functools
import pickle
import sys
from asyncio import Future
from typing import List, Union

import dill
import torch
import torch.distributed.autograd as dist_autograd
from grpclib.client import Channel
from torch import optim
from torch.distributed.optim import DistributedOptimizer

import common
from proto.helloworld.helloworld_grpc import GreeterStub
from proto.helloworld.helloworld_pb2 import *

parser = argparse.ArgumentParser(description='iris backend client.')
parser.add_argument('-r','--rank',type=int,default=0,help='rank')
parser.add_argument('-n','--name',type=str,default='node0',help='name')

class IrisOptimizerContext:
    def __init__(self, master_channel, models):
        super().__init__()
        self.channel = master_channel
        self.models = models

    async def get_parameter(self, model):
        print("get params", model)
        if model.inner.node == "node0":
            return await self.channel.GetParameter(GetParameterRequest(
                object_id = model.inner.inner.id
            ))
        return await self.channel.TorchCall(TorchRpcCallRequest(
            target_node = model.inner.node,
            object_id = model.inner.inner.id,
            torch_func = "torch_GetParameters",
            to_here = True
        ))

    async def __aenter__(self):
        a = dist_autograd.context
        torch_autograd_context:NodeObjectRef = await self.channel.CreateObject(CreateRequest(
            module=a.__module__,
            qualname=a.__qualname__
        ))
        self.torch_autograd_context = AsyncRemoteObject(self.channel, "node0", "node0", torch_autograd_context)
        self.context_id = await self.torch_autograd_context.__enter__()

        self.model_parameters = [await self.get_parameter(m) for m in self.models]
        a = DistributedOptimizer
        b = optim.SGD

        parameters = await self.channel.Apply(ApplyRequest(
            func=dill.dumps(lambda *x: functools.reduce(lambda a,b:a+b,x)),
            arg=CallArgs(
                args=pickle.dumps([common.ObjectId(x.id) for x in self.model_parameters]),
                recursive=True
            )
        ))

        arg = (ModuleRef(module=b.__module__,qualname=b.__qualname__), parameters)
        self.optimizer = await self.channel.CreateObject(CreateRequest(
            module=a.__module__,
            qualname=a.__qualname__,
            arg=CallArgs(
                args=pickle.dumps(arg),
                kwargs=pickle.dumps({
                    'lr':0.1
                })
            )
        ))
        self.optimizer = AsyncRemoteObject(self.channel, "node0", "node0", self.optimizer)
        
        return IrisOptimizer(self.context_id, self.optimizer)

    async def __aexit__(self, type, value, track):
        await self.torch_autograd_context.__exit__(None, None, None)

class IrisOptimizer:
    def __init__(self, context_id, optimizer):
        super().__init__()
        self.context_id = context_id
        self.optimizer = optimizer

    async def backward(self, loss):
        a = dist_autograd.backward
        await self.optimizer.channel.CreateObject(CreateRequest(
            module=a.__module__,
            qualname=a.__name__,
            arg=CallArgs(
                args=pickle.dumps((common.ObjectId(self.context_id.id),[common.ObjectId(loss.id)])),
                recursive=True
            )
        ))

    async def step(self):
        await self.optimizer.step(self.context_id)

class IrisModel:
    def __init__(self, model_ro, master_channel, master):
        self.inner = model_ro
        self.master_channel = master_channel
        self.master = master
        pass

    async def forward(self, *args, **kwargs):
        a = []
        cur_node = None
        call_this = self.inner.node == self.master
        for arg in args:
            if type(arg) is AsyncRemoteObject:
                if arg.node == self.inner.node:
                    a.append(common.ObjectId(arg.inner.id))
                else:
                    r_a = await self.master_channel.TorchCall(TorchRpcCallRequest(
                        target_node=arg.node,
                        object_id=arg.inner.id,
                        torch_func="torch_GetObject",
                        to_here=True
                    ))
                    a.append(common.ObjectId(r_a.id))
            else:
                a.append(arg)
        args = tuple(a)
        if call_this:
            return await self.inner.forward(*args, **kwargs)
        else:
            result = await self.master_channel.TorchCall(TorchRpcCallRequest(
                target_node = self.inner.node,
                object_id = self.inner.inner.id,
                arg = CallArgs(
                    args=pickle.dumps(args),
                    kwargs=None
                ),
                to_here=True
            ))
            return AsyncRemoteObject(self.master_channel, self.master, self.master, result)


class Context:
    def __init__(self):
        super().__init__()

    def __await__(self):
        yield [1]

class AsyncRemoteObject:
    def __init__(self, channel, node, master, inner: Union[NodeObjectRef, NodeObject]):
        super().__init__()
        self.channel = channel
        self.inner = inner
        self.id = inner.id
        self.master = master
        self.node = node
    
    async def __call__(self, *args, **kwargs):
        args = tuple(map(lambda x: x.inner if type(x) is AsyncRemoteObject else x, args))
        reply: NodeObject = await self.channel.Call(CallRequest(
        object_id = self.inner.id,
        arg=CallArgs(
                args=pickle.dumps(args),
                kwargs=pickle.dumps(kwargs)
            )
        ))
        if reply.exception:
            raise pickle.loads(reply.exception)
        return AsyncRemoteObject(self.channel,self.node,self.master,reply)

    def __getattr__(self, attr):
        if attr == 'to_here' and 'RRef' not in self.inner.type:
            async def _t():
                return self
        else:
            async def _t():
                result = await self.channel.GetAttr(GetAttrRequest(
                    object_id = self.inner.id,
                    attr=attr
                ))
                return AsyncRemoteObject(self.channel, self.node, self.master, result)
        return AsyncRemoteTask(self.channel, self.node, self.master, _t)

    async def get_value(self):
        if NodeObject is type(self.inner):
            if self.inner.WhichOneof('value') == 'id':
                raise NotImplementedError()
            elif self.inner.WhichOneof('value') == 'u64':
                return self.inner.u64
            else:
                raise NotImplementedError()
        else:
            raise NotImplementedError()

class AsyncRemoteTask:
    def __init__(self, channel, node, master, task):
        super().__init__()
        self.channel = channel
        self.task = task
        self.node = node

    def __call__(self, *args, **kwargs):
        async def _r():
            inner = await self.task()
            return await inner(*args, **kwargs)
        return ChainableTask(_r())

class ChainableTask:
    def __init__(self, task):
        self.task = task

    def __await__(self):
        return self.task.__await__()
    
    def __getattr__(self, attr):
        async def new_task():
            result = await self.task
            
        pass

async def main(args):
    master = "node0"
    channel = Channel(path=f"tmp/iris-tmp-node0-0.sock")
    channel2 = Channel(path=f"/tmp/iris-tmp-node1-1.sock")
    greeter = GreeterStub(channel)
    greeter2 = GreeterStub(channel2)

    reply: NodeObjectRef = await greeter.Init(InitRequest(
        modules=sys.modules.keys(),
        rank=0
    ))
    await greeter2.Init(InitRequest(
        modules=sys.modules.keys(),
        rank=1
    ))

    print(reply)

    a = torch.nn.Linear

    reply: NodeObjectRef = await greeter.CreateObject(CreateRequest(
            module=a.__module__,
            qualname=a.__qualname__,
            arg=CallArgs(
                args=pickle.dumps((1,1)),
                kwargs=None
            )
        ))

    print(reply)

    reply2: NodeObjectRef = await greeter2.CreateObject(CreateRequest(
            module=a.__module__,
            qualname=a.__qualname__,
            arg=CallArgs(
                args=pickle.dumps((1,1)),
                kwargs=None
            )
        ))

    model = AsyncRemoteObject(greeter, "node0", master, reply)
    model2 = AsyncRemoteObject(greeter2, "node1", master, reply2)
    model = IrisModel(model, greeter, master)
    model2 = IrisModel(model2, greeter, master)

    async with IrisOptimizerContext(greeter, [model, model2]) as optim:
        result = await model.forward(torch.zeros(10, 1))
        result2 = await model2.forward(torch.zeros(10, 1))
        # result = await model.forward(result)
        result = await result.add(result2)
        test = result.to_here().sum()
        print(test)
        # result = await result.to_here().sum()

        # await optim.backward(result)
        # await optim.step()

    print("what", result)

    # print(reply)

    channel.close()

async def what():
    c = Context()
    test = await c
    print(test)

if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(main(args))
    # asyncio.run(what())
