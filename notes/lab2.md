### 测试代码分析

先分析测试代码模拟出来的网络环境

网络中的组件有
+ network：  *表示整个网络环境*
+ server：   *表示网络环境中的某台服务器，在代码中用map保存* 
+ service：  *表示服务器中具体的某个服务，在代码中用map保存*
+ endpoint:  *类似服务器保有的socket*

每个测试用例发起时会先调用make_config()配置网络环境初始化网络配置参数，
在network初始化时启动一个goroutine 监听网络 endCh；所有网络中的请求都会通过这个无缓冲通道

在peers数组中, index = me 的endpoint 会指向本server 其他endpoint会指向另外的server
一个server 对应有server数量的endpoint;
下面的map表示endpoint和server的映射
```
connections = {
JvklEx1amGZHZSGjSNkU:0 
QWKr-e6wYzZt3IYxixJS:0 
kStJkxGoeHDQ-sEpaCAF:0 
mS7wXhpOzROccJp1kMtW:1 
p5_QrPVam8vt43U6ZvPQ:1 
Mz76y9s1fXWgGDfdFZ55:1 
51RLw_xWk03q8_tZcZmi:2 
G8ldLKRU8xwtfgubJgq6:2 
wEA1mIr8pGvpTAEW4FcB:2
]
```
如果0号server的peers为
```
[JvklEx1amGZHZSGjSNkU,mS7wXhpOzROccJp1kMtW,51RLw_xWk03q8_tZcZmi]
```
此时0号server如果要联系1号server就会通过mS7wXhpOzROccJp1kMtW这个endpoint
在part2A中每个server的services中保有一个Raft结构体指针,给不同的server发送rpc请求会由不同的Raft结构体
处理并改变自身的状态