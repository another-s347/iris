import word_count
import pickle
import sys
import torch

c = word_count.IrisContext()
c.setup()

model = c.create_object("node1", torch.nn.Linear, False, 1, 1)
model2 = c.create_object("node2", torch.nn.Linear, False, 1, 1)

with word_count.IrisGradContext(c, "node0") as context_id:
    optimizer = word_count.IrisOptimizer(c, "node0", [model2, model], context_id)
    model.weight.data.fill_(-0.4321)
    model.bias.data.fill_(0.4321)
    model2.weight.data.fill_(-0.2345)
    model2.bias.data.fill_(0.2345)
    model = word_count.IrisModel(model, c, optimizer)
    model2 = word_count.IrisModel(model2, c, optimizer)
    result = model2(torch.zeros(10, 1))
    result = model(result)
    # result3 = model(result2)
    # loss = result * result2
    loss = result.sum()
    optimizer.backward(loss)
    optimizer.step()
    result4 = model(torch.zeros(10, 1))
    result5 = model2(torch.zeros(10, 1))