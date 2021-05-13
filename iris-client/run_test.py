import client
import pickle
import sys
import torch
import net
from torchvision import datasets, transforms
import torch.nn.functional as F
import itertools
import time
from client import remote

config = client.IrisConfig()
# config.go_async = True
# config.go_async_sequence = True
config.go_async = True
config.go_async_sequence = True
config.debug = False
config.log_color = False

c = client.IrisContext(config)

node0 = c.create_node("node0",ip="10.0.0.1",port=12345)
node1 = c.create_node("node1",ip="10.0.0.2",port=12345)
node0.connect(node1, bi=True)

model = c.create_object("node0", net.Net1)
model2 = c.create_object("node1", net.Net2)

dataset = c.create_object("node0", datasets.MNIST, "../data", train=True, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                       ]))
train_loader = c.create_object("node0", torch.utils.data.DataLoader, dataset, batch_size=64)
test_dataset = c.create_object("node0", datasets.MNIST, "../data", train=False, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                       ]))
test_loader = c.create_object("node0", torch.utils.data.DataLoader, test_dataset, batch_size=64)
optimizer = c.create_object("node0", torch.optim.SGD, model.parameters(), lr=0.1)
optimizer2 = c.create_object("node1", torch.optim.SGD, model2.parameters(), lr=0.1)
test_loss = 0.
len_testdataset = len(test_loader.dataset)

@remote()
def compute_correct(result, target):
    pred = result.argmax(dim=1, keepdim=True)
    return pred.eq(target.view_as(pred)).sum().item()

nll_loss = client.RemoteTensorFunction(F.nll_loss)

start = time.time()

for epoch in range(1):
    for batch_idx, data in enumerate(train_loader):
        optimizer.zero_grad()
        optimizer2.zero_grad()
 
        t_model = client.IrisModel(model)
        t_model2 = client.IrisModel(model2)
        data, target = data[0], data[1]
        result = model(data)
        result2 = model2(result)
        loss = nll_loss.on(result2.node)(result2, target)
        loss.backward()
        optimizer.step()
        optimizer2.step()

        loss_value = loss.get()

        if batch_idx % 10 == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss:{:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader),loss_value))

    correct = 0.
    for batch_idx, data in enumerate(test_loader):
        data, target = data[0], data[1]
        output = model(data)
        result = model2(output)
        correct += compute_correct.on(result.node)(result, target).get()
        test_loss /= len_testdataset

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)'.format(
            test_loss, correct, len_testdataset,
            100. * correct / len_testdataset))

end = time.time()
print(end-start)
c.close()