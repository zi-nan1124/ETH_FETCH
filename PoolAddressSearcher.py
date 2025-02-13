import pandas as pd
from web3 import Web3
def print_error(message):
    # 红色的 ANSI 转义字符代码是 31
    print(f"\033[31m{message}\033[0m")  # 31 是红色，0 是重置颜色


class PoolAddressSearcher:
    def __init__(self, rpc_url, tokenA, tokenB, dex, factory_address, factory_abi, function_call, enable_logging=True):
        """
        初始化 PoolAddressSearcher 类
        :param rpc_url: 以太坊节点的 RPC URL
        :param tokenA: 第一个代币地址
        :param tokenB: 第二个代币地址
        :param dex: DEX 名称
        :param factory_address: 工厂合约地址
        :param factory_abi: 工厂合约的 ABI
        :param function_call: 调用的函数名称
        :param enable_logging: 是否启用日志输出
        """
        self.enable_logging = enable_logging
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        if self.web3.is_connected():
            self.log("Connected to Ethereum node")
        else:
            print_error("Failed to connect")
        self.tokenA = self.web3.to_checksum_address(tokenA)
        self.tokenB = self.web3.to_checksum_address(tokenB)
        self.dex = dex
        self.factory_address = self.web3.to_checksum_address(factory_address)
        self.factory_abi = factory_abi
        self.function_call = function_call
        self.log(f"[INIT] DEX: {self.dex}, TokenA: {self.tokenA}, TokenB: {self.tokenB}")

    def log(self, message):
        """控制日志输出的函数."""
        if self.enable_logging:
            print(message)

    def query_pool_address(self):
        """
        查询池地址
        :return: 查询的池地址或 None
        """
        try:
            self.log(f"[QUERY] Querying pool address for factory: {self.factory_address}")
            # 初始化工厂合约
            factory_contract = self.web3.eth.contract(address=self.factory_address, abi=self.factory_abi)

            # 根据 function_call 调用函数
            if self.function_call == "factory_contract.functions.getPool(tokenA, tokenB, 500).call()":
                pool_address = factory_contract.functions.getPool(self.tokenA, self.tokenB, 500).call()
            elif self.function_call == "factory_contract.functions.getPool(tokenA, tokenB, 3000).call()":
                pool_address = factory_contract.functions.getPool(self.tokenA, self.tokenB, 3000).call()
            elif self.function_call == "factory_contract.functions.getPool(tokenA, tokenB, 10000).call()":
                pool_address = factory_contract.functions.getPool(self.tokenA, self.tokenB, 10000).call()
            elif self.function_call == "factory_contract.functions.getPair(tokenA, tokenB).call()":
                pool_address = factory_contract.functions.getPair(self.tokenA, self.tokenB).call()
            else:
                raise ValueError(f"Unsupported function call: {self.function_call}")

            # 检查查询到的池地址是否为空
            if pool_address == "0x0000000000000000000000000000000000000000" or not pool_address:
                self.log("[QUERY WARNING] Pool address is empty or invalid.")
                return None

            self.log(f"[QUERY SUCCESS] Pool address: {pool_address}")
            return pool_address
        except Exception as e:
            print_error(f"[QUERY ERROR] Error querying pool address: {e}")
            return None

    def process_data(self):
        """
        返回包含 dex、tokenA、tokenB 和 pool_address 的 pandas DataFrame
        """
        self.log("[PROCESS] Processing data...")
        pool_address = self.query_pool_address()
        data = {
            "dex": [self.dex],
            "tokenA": [self.tokenA],
            "tokenB": [self.tokenB],
            "pool_address": [pool_address],
        }
        self.log(f"[PROCESS SUCCESS]")
        return pd.DataFrame(data)


if __name__ == "__main__":
    # 配置参数
    rpc_url = "https://mainnet.infura.io/v3/df534c99ab44499d87b5378081639195"
    input_csv = "factory.csv"
    output_csv = "search_pooladdr_bypair.csv"
    enable_logging = True  # 控制日志输出开关

    # 从输入 CSV 文件读取数据
    df = pd.read_csv(input_csv)

    # 用于存储结果的 pandas DataFrame
    all_results = pd.DataFrame()

    # 遍历输入 CSV 文件的每一行并调用 PoolAddressSearcher
    for index, row in df.iterrows():
        try:
            # 获取当前行的数据
            dex = row["dex"]
            factory_address = row["factoryaddress"]
            factory_abi = eval(row["factoryabi"])
            function_call = row["getfuction"]

            # 示例交易对：指定 tokenA 和 tokenB
            tokenA = "0xdAC17F958D2ee523a2206206994597C13D831ec7" # USDT
            tokenB = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599" # WETH

            # 初始化 PoolAddressSearcher 并处理数据
            searcher = PoolAddressSearcher(
                rpc_url, tokenA, tokenB, dex, factory_address, factory_abi, function_call, enable_logging
            )
            result_df = searcher.process_data()

            # 合并结果
            all_results = pd.concat([all_results, result_df], ignore_index=True)
        except Exception as e:
            if enable_logging:
                print(f"[ERROR] Error processing row {index}: {e}")

    # 保存所有结果到 CSV 文件
    all_results.to_csv(output_csv, index=False)
    if enable_logging:
        print(f"[SUCCESS] Results saved to {output_csv}")
