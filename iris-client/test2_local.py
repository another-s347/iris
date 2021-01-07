import pickle
import sys
import torch
import net
from torchvision import datasets, transforms
import torch.nn.functional as F
import itertools

model =  net.Net1()
model2 = net.Net2()

dataset = datasets.MNIST("../data", train=True, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                       ]))
train_loader = torch.utils.data.DataLoader(dataset, batch_size=64)
test_dataset = datasets.MNIST("../data", train=False, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                       ]))
test_loader = torch.utils.data.DataLoader(test_dataset, batch_size=64)
optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
optimizer2 = torch.optim.SGD(model2.parameters(), lr=0.1)
correct = 0.
test_loss = 0.
len_testdataset = len(test_loader.dataset)

for epoch in range(1):
    for batch_idx, data in enumerate(train_loader):
        a = optimizer.zero_grad()
        b = optimizer2.zero_grad()
 
        data, target = data[0], data[1]
        result = model(data)
        result_r = result.clone().detach().requires_grad_(True)
        result2 = model2(result_r)
        loss = F.nll_loss(result2, target)

        loss_value = loss
        loss.backward()
        result.backward(result_r.grad)

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
        correct += pred.eq(target.view_as(pred)).sum().item()
        test_loss /= len_testdataset

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)'.format(
            test_loss, correct, len_testdataset,
            100. * correct / len_testdataset))
