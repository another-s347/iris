import asyncio
import socket
from grpclib.utils import graceful_exit
from grpclib.server import Server
import dill
import pickle
import os
import torch
from torch.distributed.rpc import RRef
import torch.distributed.rpc as rpc
import importlib
# generated by protoc
from proto.helloworld.helloworld_pb2 import *
from proto.helloworld.helloworld_grpc import GreeterBase
import argparse
# import common
import functools
import sys
import uuid
import time

parser = argparse.ArgumentParser(description='iris backend client.')
parser.add_argument('-r', '--rank', default=0, type=int, help='rank')
parser.add_argument('-n', '--name', default='node0', type=str, help='name')

# torch_CallWithObject = None
service = None


def torch_GetParameters(object_id, attr, *args, **kwargs):
    global service
    # print("torch_GetParameters", object_id)
    model = service.object_map[object_id]
    return [rpc.RRef(p) for p in model.parameters()]


def torch_CallWithObject(object_id, attr, *args, **kwargs):
    global service
    o = service.object_map[object_id]
    if attr:
        o = getattr(o, attr)
    result = o(*args, **kwargs)
    return result


def torch_GetObject(object_id, attr, *args, **kwargs):
    global service
    o = service.object_map[object_id]
    return o


class Greeter(GreeterBase):
    def __init__(self, args):
        super().__init__()
        global service
        self.args = args
        self.object_map = {}
        self.modules = {}
        self.object_map[id(self.modules)] = self.modules
        self.is_master = args.rank == 0
        self.loop = asyncio.get_running_loop()
        if service:
            raise NotImplementedError()
        service = self

    async def Init(self, stream):
        request: InitRequest = await stream.recv_message()
        path = request.paths
        sys.path += path
        for m in request.modules:
            try:
                self.modules[m] = importlib.import_module(m)
            except Exception as e:
                print(e)
        await stream.send_message(NodeObject(
            type="init",
            id=id(self.modules),
            location=self.args.name
        ))

    async def GetParameter(self, stream):
        request: GetParameterRequest = await stream.recv_message()
        model = self.object_map[request.object_id]
        parameters = [rpc.RRef(p) for p in model.parameters()]
        key = hash(uuid.uuid4().int)
        self.object_map[key] = parameters
        result = NodeObject(location=self.args.name,type=str(type(parameters)))
        result.id = key
        await stream.send_message(result)

    async def Apply(self, stream):
        request: ApplyRequest = await stream.recv_message()
        args = pickle.loads(
            request.arg.args) if request.arg and request.arg.args else ()
        recursive = request.arg.recursive if request.arg else False
        targs = tuple(map(self.map_args_to_local(False), args))
        func = dill.loads(request.func)
        ret = func(*targs)
        result = NodeObject(
            location=self.args.name,type=str(type(ret))
        )
        key = hash(uuid.uuid4().int)
        result.id = key
        self.object_map[key] = ret
        await stream.send_message(result)

    async def TorchCall(self, stream):
        request: TorchCallRequest = await stream.recv_message()
        args = pickle.loads(
            request.arg.args) if request.arg and request.arg.args else ()
        recursive = request.arg.recursive if request.arg else False
        args = tuple(map(self.map_args_to_local(recursive), args))
        handler = name_to_torch_handler(request.torch_func)
        ret = rpc.remote(request.target_node, handler, args=(
            request.object_id, request.attr, *args))
        if request.to_here:
            # a = time.time()
            ret = ret.to_here()
            # print(time.time()-a)
            # ret = await self.loop.run_in_executor(None, ret.to_here)
        key = hash(uuid.uuid4().int)
        self.object_map[key] = ret
        result = NodeObject(
            location=self.args.name,type=str(type(ret))
        )
        result.id = key
        await stream.send_message(result)

    async def Call(self, stream):
        request: CallRequest = await stream.recv_message()
        args = pickle.loads(
            request.arg.args) if request.arg and request.arg.args else ()
        recursive = request.arg.recursive if request.arg else False
        args = tuple(map(self.map_args_to_local(recursive), args))
        kwargs = pickle.loads(request.arg.kwargs) if request.arg and request.arg.kwargs else {}
        result = NodeObject(
            location=self.args.name
        )
        try:
            o = self.object_map[request.object_id]
        except KeyError as e:
            result.exception = pickle.dumps(e)
            await stream.send_message(result)
            return
        if request.attr:
            o = getattr(o, request.attr)
        if callable(o):
            try:
                if isinstance(o, torch.nn.Module):
                    def run():
                        return o(*args, **kwargs)
                    ret = await self.loop.run_in_executor(None, run)
                else:
                    ret = o(*args, **kwargs)
                key = hash(uuid.uuid4().int)
                result.id = key
                result.type = str(type(ret))
                self.object_map[key] = ret
            except Exception as e:
                print(e)
                print(args)
                result.exception = pickle.dumps(e)
        else:
            result.exception = pickle.dumps(
                    TypeError(f"{type(o)} is not callable."))
        await stream.send_message(result)

    async def CreateObject(self, stream):
        request: CreateRequest = await stream.recv_message()
        # cls_name = request.cls.split('.')
        m = self.modules[request.module]
        m = getattr(m, request.qualname)
        # print(self.modules['torch.nn.modules.linear'])
        # for c in cls_name[1:]:
        #     m = getattr(m, c)
        args = pickle.loads(
            request.arg.args) if request.arg and request.arg.args else ()
        recursive = request.arg.recursive if request.arg else False
        try:
            args = tuple(map(self.map_args_to_local(recursive), args))
            kwargs = pickle.loads(
                request.arg.kwargs) if request.arg and request.arg.kwargs else {}
            obj = m(*args, **kwargs)
            key = hash(uuid.uuid4().int)
            self.object_map[key] = obj
            await stream.send_message(NodeObject(
                id=key,
                location=self.args.name,
                type=str(type(obj))
            ))
        except Exception as e:
            print(args)
            print("Error when handling", request)
            print(e)
        # return super().CreateObject(stream)

    async def GetAttr(self, stream):
        request: GetAttrRequest = await stream.recv_message()
        o = self.object_map[request.object_id]
        a = getattr(o, request.attr)
        result = NodeObject(
            location=self.args.name,type=str(type(a))
        )
        key = hash(uuid.uuid4().int)
        result.id = key
        self.object_map[key] = a
        await stream.send_message(result)

    async def SayHello(self, stream):
        request: HelloRequest = await stream.recv_message()
        message = f'Hello, {request.name}!'
        await stream.send_message(HelloReply(message=message))

    async def GetValue(self, stream):
        request: GetParameterRequest = await stream.recv_message()
        o = self.object_map[request.object_id]
        ret = Value(data=pickle.dumps(o))
        await stream.send_message(ret)

    def map_args_to_local(self, recursive):
        """
        Map deserialized arguments to local value.

        Args:
            1. Module reference
            2. Object reference
            3. Value
        Parameters:
        x: deserialized arguments
        """
        def _r_impl(x):
            if NodeObject is type(x) and x.WhichOneof('value') == 'id':
                return self.object_map[x.id]
            elif NodeObject is type(x):
                return self.object_map[x.id]
            elif ModuleRef is type(x):
                m = self.modules[x.module]
                return getattr(m, x.qualname)
            elif 'ObjectId' in str(type(x)):
                return self.object_map[x.id]
            elif recursive and list is type(x):
                return list(map(self.map_args_to_local(recursive), x))
            else:
                return x
        return _r_impl

    async def DelObject(self, stream):
        request = await stream.recv_message()
        if request.object_id in self.object_map:
            del self.object_map[request.object_id]
        await stream.send_message(ObjectId(id=request.object_id))


async def main(args):
    os.environ['MASTER_ADDR'] = '127.0.0.1'
    os.environ['MASTER_PORT'] = '29500'
    rpc.init_rpc(f"{args.name}", rank=args.rank, world_size=3)
    server = Server([Greeter(args)])
    with graceful_exit([server]):
        await server.start(path=f"/tmp/iris-tmp-{args.name}-{args.rank}.sock")
        print(f'Serving on /tmp/iris-tmp-{args.name}-{args.rank}.sock')
        await server.wait_closed()


def name_to_torch_handler(name):
    if name == 'torch_GetParameters':
        return torch_GetParameters
    if name == 'torch_GetObject':
        return torch_GetObject
    return torch_CallWithObject


if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(main(args))
