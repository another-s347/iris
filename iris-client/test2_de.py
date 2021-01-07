import client
import pickle
import sys
import torch
import net
from torchvision import datasets, transforms
import torch.nn.functional as F
import itertools

config = client.IrisConfig()
# config.go_async = True
# config.go_async_sequence = True
config.go_async = False
config.go_async_sequence = False
config.debug = False
config.log_color = False

c = client.IrisContext(config)
c.setup()

model = c.create_object("node0", net.Net1)
model2 = c.create_object("node1", net.Net2)

def check(a):
    return a.shape

dataset = c.create_object("node0", datasets.MNIST, "../data", train=True, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                       ]))
train_loader = c.create_object("node0", torch.utils.data.DataLoader, dataset, batch_size=64)
test_dataset = c.create_object("node0", datasets.MNIST, "../data", train=False, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                       ]))
test_loader = c.create_object("node0", torch.utils.data.DataLoader, test_dataset, batch_size=64)
optimizer = c.create_object("node0", torch.optim.SGD, model.parameters(), lr=0.1)
optimizer2 = c.create_object("node1", torch.optim.SGD, model2.parameters(), lr=0.1)
correct = 0.
test_loss = 0.
len_testdataset = len(test_loader.dataset)

c.client_wrapper["node0"].apply(
            func = torch.autograd.set_detect_anomaly,
            args = (True,),
            kwargs = None
        )

c.client_wrapper["node1"].apply(
            func = torch.autograd.set_detect_anomaly,
            args = (True,),
            kwargs = None
        )

for epoch in range(1):
    for batch_idx, data in enumerate(train_loader):
        optimizer.zero_grad()
        optimizer2.zero_grad()
 
        data, target = data[0], data[1]
        result = model(data)
        result_r = result.to_node(model2.node).clone().detach().requires_grad_()
        result2 = model2(result_r)
        loss = c.client_wrapper[result2.node].apply(
            func = F.nll_loss,
            args = (result2, target),
            kwargs = None
        )

        loss_value = loss.get()
        loss.backward()
        # # print(grad.size())
        result.backward(result_r.grad).get()

        optimizer.step()
        optimizer2.step()

        if batch_idx % 10 == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss:{:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader),loss_value))

    for batch_idx, data in enumerate(test_loader):
        data, target = data[0], data[1]
        output = model(data)
        result = model2(output)
        pred = result.argmax(dim=1, keepdim=True)
        pred2 = pred.detach()
        pred3 = pred2.get()
        target = target.get()
        correct += pred3.eq(target.view_as(pred3)).sum().item()
        test_loss /= len_testdataset

        loss = c.client_wrapper[result.node].apply(
            func = F.nll_loss,
            args = (result, target),
            kwargs = None
        )

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)'.format(
            test_loss, correct, len_testdataset,
            100. * correct / len_testdataset))
