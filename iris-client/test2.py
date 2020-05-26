import word_count
import pickle
import sys
import torch
import net
from torchvision import datasets, transforms
import torch.nn.functional as F

c = word_count.IrisContext()
c.setup()

model = c.create_object("node1", net.Net1, False)
model2 = c.create_object("node2", net.Net2, False)

dataset = c.create_object("node0", datasets.MNIST, False, "../data", train=True, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                       ]))
train_loader = c.create_object("node0", torch.utils.data.DataLoader, False, dataset, batch_size=64)
test_dataset = c.create_object("node0", datasets.MNIST, False, "../data", train=False, download=True, transform=transforms.Compose([
                           transforms.ToTensor(),
                           transforms.Normalize((0.1307,), (0.3081,))
                       ]))
test_loader = c.create_object("node0", torch.utils.data.DataLoader, False, test_dataset, batch_size=64)
correct = 0.
test_loss = 0.
len_testdataset = len(test_loader.dataset)
for epoch in range(3):
    for batch_idx, data in enumerate(train_loader):
        data, target = data[0], data[1]
        with word_count.IrisGradContext(c, "node0") as context_id:
            optimizer = word_count.IrisOptimizer(c, "node0", [model, model2], context_id)
            train_model = word_count.IrisModel(model, c, optimizer)
            train_model2 = word_count.IrisModel(model2, c, optimizer)
            result = train_model(data)
            result = train_model2(result)
            loss = c.client_wrapper[result.node].apply(
                func = F.nll_loss,
                args = (result, target),
                kwargs = None,
                recursive = False
            )
            optimizer.backward(loss)
            optimizer.step()
            if batch_idx % 10 == 0:
                print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                    epoch, batch_idx * len(data), len(train_loader.dataset),
                    100. * batch_idx / len(train_loader), loss.get()))

    for batch_idx, data in enumerate(test_loader):
        data, target = data[0], data[1]
        output = model(data)
        result = model2(output)
        pred = result.argmax(dim=1, keepdim=True).detach().get()
        target = target.get()
        correct += pred.eq(target.view_as(pred)).sum().item()
        test_loss /= len_testdataset

    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)'.format(
            test_loss, correct, len_testdataset,
            100. * correct / len_testdataset))
