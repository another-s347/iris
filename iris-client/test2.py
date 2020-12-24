import client
import pickle
import sys
import torch
import net
from torchvision import datasets, transforms
import torch.nn.functional as F

c = client.IrisContext()
c.setup()

model = c.create_object("node0", net.Net1)
model2 = c.create_object("node1", net.Net2)

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
for epoch in range(3):
    for batch_idx, data in enumerate(train_loader):
        optimizer.zero_grad()
        optimizer2.zero_grad()
        data, target = data[0], data[1]
        train_model = client.IrisModel(model)
        train_model2 = client.IrisModel(model2)
        result = train_model(data)
        result = train_model2(result)

        loss = c.client_wrapper[result.inner.node].apply(
            func = F.nll_loss,
            args = (result.inner, target),
            kwargs = None
        )
        loss.backward()
        optimizer.step()
        optimizer2.step()
        loss = loss.get()
        if batch_idx % 10 == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss:{:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader),loss))

    for batch_idx, data in enumerate(test_loader):
        data, target = data[0], data[1]
        output = model(data)
        result = model2(output)
        pred = result.argmax(dim=1, keepdim=True).detach().get()
        target = target.get()
        correct += pred.eq(target.view_as(pred)).sum().item()
        test_loss /= len_testdataset

        print(result, target)

        loss = c.client_wrapper[result.node].apply(
            func = F.nll_loss,
            args = (result, target),
            kwargs = None
        )

        print(loss.get())

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)'.format(
            test_loss, correct, len_testdataset,
            100. * correct / len_testdataset))
