# README

## 项目简介

该项目包含多个功能模块，用于从DEX批量获取交易池数据、提取并解码交易日志、以及对数据进行计算分析，最终根据需要的时间间隔生成k线数据。主要任务包括以下几个部分：

1. **池地址查询**：根据工厂地址和代币对信息查询流动性池地址。
2. **日志提取与解码**：从区块链获取交易日志并解码。
3. **数据处理与计算**：对提取的数据进行计算分析，并输出结果。

## 目录结构

```bash
.
├── INPUT
│   ├── factory.csv    # 工厂地址数据
│   └── pair.csv       # 代币对信息
├── RESULT
│   ├── search_pooladdr_bypair.csv  # 查找的池地址结果,项目的索引表格
│   └── decoded_logs.csv            # 解码后的交易日志
├── DEXLogExtractor.py  # 提取和解码交易日志的模块
├── PoolAddressSearcher.py  # 查找池地址的模块
├── Calculator.py         # 计算和数据处理的模块
└── ETHFetch.py               # 主程序入口
```

## 使用指南

### 环境要求

- Python 3.x
- `web3.py` (安装方法：`pip install web3`)
- `eth-abi` (安装方法：`pip install eth-abi`)
- `pandas` (安装方法：`pip install pandas`)

### 配置文件

1. `factory.csv`：包含工厂地址、ABI 和查询函数的信息。
2. `pair.csv`：包含代币对信息，包括代币 A 和代币 B 的地址及名称。

### 配置参数

- **`rpc_url`**：区块链节点的 RPC URL，用于连接到以太坊或其他支持的区块链。
- **`input_csv1`**：包含工厂数据的 CSV 文件路径，通常包含有关 DEX 工厂的详细信息。
- **`input_csv2`**：包含交易对数据的 CSV 文件路径，通常包含有关交易对的详细信息。
- **`output_path`**：结果输出路径，用于存储处理后的数据。
- **`enable_logging`**：布尔值，指示是否启用日志记录，`True` 表示启用，`False` 表示禁用。

### 执行步骤

1. **查询池地址**：根据工厂地址和代币对信息，程序会查询流动性池地址，并将结果保存到 `RESULT/search_pooladdr_bypair.csv`。

2. **提取交易日志并解码**：根据查询到的池地址，从区块链提取交易日志并解码，结果会保存在 `RESULT/DEX_name-tokenA-tokenB.csv`。

3. **数据计算**：根据提取的日志数据进行计算分析，包括交易量、平均价格等，并将结果保存到 `RESULT/tokenA-tokenB-interval.csv` 文件中。

### 日志输出
- 在主程序DEX.py中，变量 enable_logging 的值是日志模式的开关，TRUE代表着开
- 程序会在控制台输出日志信息，帮助用户追踪程序执行过程。
- 包含日志内容的文件将保存在 `RESULT/` 文件夹中。

### 错误处理

- 如果在池地址查询、日志提取或数据处理过程中遇到错误，程序会输出相应的错误信息，并跳过有问题的数据。

## 功能模块

### 1. `DEXLogExtractor`

该模块负责从区块链获取交易日志并解码。它支持获取指定时间范围内的交易数据，并解析相关信息。

### 2. `PoolAddressSearcher`

该模块负责查询流动性池的地址。它使用指定的工厂地址和代币对信息来查询池地址。

### 3. `Calculator`

该模块负责处理交易数据，计算交易量、价格等信息，并保存结果。通过调用 `calculate()` 方法，用户可以处理数据并生成最终的结果。

## 示例

### 添加新DEX示例

#### `factory.csv`

```csv
dex,factoryaddress,factoryabi,getfuction
uniswap_v2,0x5C69bEe701ef814a2B6a3EDD4B8a3Bf36c6f980,["ABI_JSON_HERE"],getPair
```

#### `pair.csv`

```csv
tokenA,tokenB,tokenAname,tokenBname
0xdAC17F958D2ee523a2206206994597C13D831ec7,0xC02aaa39B223Fe8D0A0E5C4F27EAD9083C756Cc2,ETH,USD
```

```python
PoolAddressSearcher.py
def query_pool_address():
    添加属于新DEX的函数分支（该函数指与工厂合约互动获取交易池地址的函数）
    
    
DEXLogExtractor.py
def __init__():
    self.topic_map = {
            "PancakeSwap_v2": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
            "uniswap_v3": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
            "uniswap_v2": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        添加属于新DEX的SWAP主题的哈希值
        }
 def decode_logs(self, logs):
                 if self.dex == "PancakeSwap_v2":
                    # Pancake V3 ABI types
                    abi_types = ["uint256", "uint256", "uint256", "uint256"]
                    decoded = decode(abi_types, data)
                    log_data = {
                        "transactionHash": log["transactionHash"].hex(),
                        "amount0In": decoded[0] / 1e18,
                        "amount1In": decoded[1] / 1e18,
                        "amount0Out": decoded[2] / 1e18,
                        "amount1Out": decoded[3] / 1e18,
                        "sender": log["topics"][1].hex(),
                        "to": log["topics"][2].hex(),
                    }
                    后添加属于新dex的数据abi与解码逻辑
                    
 Calculator.py
def process_data(self, df):
            if self.dex == "uniswap_v3":
            添加属于新DEX的Volume与Price计算逻辑，计算输出只有transactionHashHash,starttime,endtime,token1,token2,volume_eth,volume_gwei,price列
```



## 贡献

欢迎提交问题和 Pull Request，帮助改进该项目。如果有任何建议或问题，请随时联系我。

## 许可证

该项目使用 [MIT 许可证](LICENSE)。
