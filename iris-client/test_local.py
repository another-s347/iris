import torch

model = torch.nn.Linear(1, 1)
model.weight.data.fill_(-0.4321)
model.bias.data.fill_(0.4321)
model2 = torch.nn.Linear(1, 1)
model2.weight.data.fill_(-0.2345)
model2.bias.data.fill_(0.2345)

optimizer = torch.optim.SGD(params=[{
    "params":model.parameters(),
    "params":model2.parameters()
}], lr=0.1)

result = model2(torch.zeros(10, 1))
result = model(result)

loss = result.sum()
for p in model.parameters():
    print(p, p.grad)
for p in model2.parameters():
    print(p, p.grad)
loss.backward()
optimizer.step()
for p in model.parameters():
    print(p, p.grad)
for p in model2.parameters():
    print(p, p.grad)
