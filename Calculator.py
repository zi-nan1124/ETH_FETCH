import pandas as pd
import os
import hashlib
from .config import CONFIG

from numpy.core.defchararray import lower
from web3 import Web3
from datetime import datetime

def print_error(message):
    # 红色的 ANSI 转义字符代码是 31
    print(f"\033[31m{message}\033[0m")  # 31 是红色，0 是重置颜色

class Calculator:
    def __init__(self, rpc_url,pooladdress,tokenA,tokenAname,tokenB, tokenBname, dex, interval, enable_logging):
        """
        初始化 Calculator 类
        :param tokenA: 第一个代币地址
        :param tokenB: 第二个代币地址
        :param dex: DEX 名称
        :param pooladdress: 流动性池地址
        :param interval: 分组间隔，支持 '1T' (分钟), '1H' (小时), '1D' (天)
        :param enable_logging: 是否启用日志输出
        """
        self.enable_logging = enable_logging
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        if self.web3.is_connected():
            self.log("Connected to Ethereum node")
        else:
            print_error("Failed to connect")
        self.dex = dex
        self.tokenA = self.web3.to_checksum_address(tokenA)
        self.tokenAname = tokenAname
        self.tokenB = self.web3.to_checksum_address(tokenB)
        self.tokenBname = tokenBname
        self.pooladdress = pooladdress
        self.interval = interval
        self.token_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "constant": True,
                "inputs": [],
                "name": "symbol",
                "outputs": [{"name": "", "type": "string"}],
                "stateMutability": "view",
                "type": "function"
            }
        ]

        self.pool_abi = [
        {
            "inputs": [],
            "name": "token0",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "token1",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        }
    ]


    def log(self, message):
        """
        日志输出方法
        :param message: 日志内容
        """
        if self.enable_logging:
            print(message)

    def load_data(self):
        """
        加载指定文件夹中的数据文件
        :return: pandas DataFrame
        """
        result_folder = CONFIG["output_path"]
        filename = f"{self.dex}-{self.tokenAname}-{self.tokenBname}.csv"
        filepath = os.path.join(result_folder, filename)

        if not os.path.exists(filepath):
            print_error(f"[ERROR] Data file not found: {filepath}")
            raise FileNotFoundError(f"Data file not found: {filepath}")

        self.log(f"[INFO] Loading data from {filepath}")
        return pd.read_csv(filepath)

    def process_data(self, df):
        """
        根据 DEX 类型处理数据，并按时间间隔分组
        :param df: 原始数据 DataFrame
        :return: 处理后的数据 DataFrame
        """
        self.log(f"[INFO] Processing data for DEX: {self.dex}")

        # 确保 timestamp 列是 datetime 格式，无需判断直接转换
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)


        # 使用 pool_address 获取 token0 和 token1 地址
        pool_contract = self.web3.eth.contract(address=self.pooladdress, abi=self.pool_abi)
        token0_address = pool_contract.functions.token0().call()
        token1_address = pool_contract.functions.token1().call()

        # 获取 tokenA 和 tokenB 的 decimals
        contract0 = self.web3.eth.contract(address=token0_address, abi=self.token_abi)
        symbol0= contract0.functions.symbol().call()
        decimals0 = contract0.functions.decimals().call()
        contract1 = self.web3.eth.contract(address=token1_address, abi=self.token_abi)
        decimals1 = contract1.functions.decimals().call()
        symbol1= contract1.functions.symbol().call()

        if self.dex == "uniswap_v3":
            # 转换为数值类型
            df["amount0"] = pd.to_numeric(df["amount0"], errors="coerce")
            df["amount1"] = pd.to_numeric(df["amount1"], errors="coerce")

            # 检查是否有无法转换为数值的值
            if df["amount0"].isna().any():
                print_error("[ERROR] Non-numeric values found in 'amount0'")
            if df["amount1"].isna().any():
                print_error("[ERROR] Non-numeric values found in 'amount1'")
            self.log(f"amount0 dtype: {df['amount0'].dtype}, amount1 dtype: {df['amount1'].dtype}")

            # 交易量计算
            df["volume0"] = df["amount0"].abs() / (10 ** decimals0)
            df["volume1"] = df["amount1"].abs() / (10 ** decimals1)
            df["symbol0"] = symbol0
            df["symbol1"] = symbol1

            # 按时间间隔分组并计算交易量
            grouped = df.resample(self.interval).agg({
                "volume0": "sum",
                "volume1": "sum",
                "transactionHash": lambda x: hashlib.sha256(''.join(x).encode()).hexdigest()
            })

            # 重命名 transactionHash 列
            grouped.rename(columns={"transactionHash": "transactionHashHash"}, inplace=True)

            # 添加 symbol1 和 symbol2 列
            grouped["symbol0"] = symbol0
            grouped["symbol1"] = symbol1



        elif self.dex in ["uniswap_v2", "PancakeSwap_v2"]:

            df["volume0"] = (abs(df["amount0In"]) + abs(df["amount0Out"])) / 10 ** (decimals0) # 稳定币交易量
            df["volume1"] = (abs(df["amount1In"]) + abs(df["amount1Out"])) / 10 ** (decimals1)  # 非稳定币交易量
            df["symbol0"] = symbol0
            df["symbol1"] = symbol1

            # 按时间间隔分组并计算交易量和平均价格
            grouped = df.resample(self.interval).agg({
                "volume0": "sum",  # 稳定币交易量
                "volume1": "sum",  # 价格的平均值
                "transactionHash": lambda x: hashlib.sha256(''.join(x).encode()).hexdigest()  # 拼接交易哈希并进行 SHA256 哈希
            })

            # 重命名 transactionHash 列
            grouped.rename(columns={"transactionHash": "transactionHashHash"}, inplace=True)

            # 添加 symbol1 和 symbol2 列
            grouped["symbol0"] = symbol0
            grouped["symbol1"] = symbol1


        else:
            print_error(f"[ERROR] Unsupported DEX type: {self.dex}")
            raise ValueError(f"Unsupported DEX type: {self.dex}")

        # 计算分组的起始时间和结束时间
        grouped["starttime"] = grouped.index
        grouped["endtime"] = grouped.index + pd.to_timedelta(self.interval)
        # 根据token0是否是稳定币动态设置 token0 和 token1
        self.tokenAname = symbol0
        self.tokenBname = symbol1
        self.log(f"[INFO] Data processed successfully for DEX: {self.dex}")
        return grouped.reset_index(drop=True)

    def save_to_csv(self, df):
        """
        保存处理后的数据到新的 CSV 文件中，并根据交易哈希去重
        :param df: 处理后的数据 DataFrame
        """
        # 检查 df 是否为空
        if df.empty:
            self.log("[WARNING] The DataFrame is empty. No data to save.")
            return  # 如果为空，输出警告并直接返回
        result_folder = CONFIG["output_path"]

        output_filename = f"{self.tokenAname}-{self.tokenBname}-{self.interval}.csv"
        output_filepath = os.path.join(result_folder, output_filename)

        # 如果文件已存在，则读取并去重；否则创建新文件
        if os.path.exists(output_filepath):
            existing_df = pd.read_csv(output_filepath)

            # 合并现有数据和新数据，按交易哈希去重
            combined_df = pd.concat([existing_df, df], ignore_index=True)

            # 根据交易哈希去重，保留第一次出现的记录
            combined_df = combined_df.drop_duplicates(subset=["transactionHashHash"], keep="first")
        else:
            combined_df = df

        # 将 transactionHashHash 移动到第一列
        columns = ["transactionHashHash"] + [col for col in combined_df.columns if col != "transactionHashHash"]
        combined_df = combined_df[columns]

        # 保存去重后的数据到 CSV
        combined_df.to_csv(output_filepath, index=False)
        self.log(f"[SUCCESS] Processed data saved to {output_filepath}")

    def merge_data(self, processed_data):
        """
        处理并合并数据：按 starttime 和 endtime 完全相同的数据合并，并处理交易哈希
        :param processed_data: 处理后的 DataFrame
        :return: 合并后的 DataFrame
        """
        self.log(f"[INFO] Merging data by starttime and endtime.")


        # 按 starttime 和 endtime 分组，交易量求和，价格求平均
        merged_data = processed_data.groupby(["starttime", "endtime", "symbol0", "symbol1"], as_index=False).agg({
            "volume0": "sum",  # 交易量之和
            "volume1": "sum",  # 价格平均值
            "transactionHashHash": lambda x: hashlib.sha256(''.join(x).encode()).hexdigest()  # 合并交易哈希并计算 SHA256 哈希
        })


        # 将 transactionHashHash 移动到第一列
        columns = ["transactionHashHash"] + [col for col in merged_data.columns if col != "transactionHashHash"]
        merged_data = merged_data[columns]

        self.log(f"[INFO] Data merged successfully.")
        return merged_data

    def calculate(self):
        """
        主计算函数，加载数据 -> 处理数据 -> 合并数据 -> 保存数据
        """
        try:
            self.log(f"[INFO] Starting calculation for DEX: {self.dex}, PAIR: {self.tokenAname}-{self.tokenBname}")
            raw_data = self.load_data()
            processed_data = self.process_data(raw_data)
            processed_data.to_csv('test.csv', index=False)
            merge_data = self.merge_data(processed_data)
            #merge_data.to_csv('test.csv', index=False)
            self.save_to_csv(merge_data)
        except Exception as e:
            print_error(f"[ERROR] Calculation failed: {e}")


if __name__ == "__main__":
    # 示例调用
    rpc_url = "https://neat-chaotic-pond.quiknode.pro/13d93177702a33b746afc3218f44d1c8d21679d0"
    dex = "uniswap_v2"
    tokenB = "0xC02aaa39b223FE8D0A0E5C4F27eAD9083C756Cc2"
    tokenBname = "WETH"
    tokenAname = "USDT"
    tokenA = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    pooladdress = "0x11b815efB8f581194ae79006d24E0d814B7697F6"
    interval = "5min"  # 时间间隔（10分钟）
    enable_logging = True  # 日志开关

    # 初始化 Calculator 并调用计算函数
    calculator = Calculator(rpc_url,pooladdress,tokenA,tokenAname, tokenB,tokenBname, dex, interval ,enable_logging)
    calculator.calculate()
