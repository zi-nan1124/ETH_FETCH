import json
from datetime import datetime
from web3 import Web3
#from web3.middleware import geth_poa_middleware
from eth_abi import decode
import binascii
import pandas as pd
import os
from .config import CONFIG
def print_error(message):
    # 红色的 ANSI 转义字符代码是 31
    print(f"\033[31m{message}\033[0m")  # 31 是红色，0 是重置颜色


class DEXLogExtractor:
    def __init__(self, rpc_url, dex, pool_address, start_time, end_time, enable_logging):
        """
        初始化提取器
        :param rpc_url: 区块链节点的 RPC URL
        :param dex: DEX 类型，例如 'pancake_v3' 或 'uniswap_v3'
        :param pool_address: 流动性池地址
        :param start_time: 查询的开始时间 (datetime 对象)
        :param end_time: 查询的结束时间 (datetime 对象)
        :param enable_logging: 是否启用日志输出 (默认启用)
        """
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        #self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
        self.dex = dex
        self.pool_address = pool_address
        self.start_time = start_time
        self.end_time = end_time
        self.enable_logging = enable_logging  # 控制日志输出的变量
        self.topic_map = {
            "PancakeSwap_v2": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
            "uniswap_v3": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
            "uniswap_v2": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
        }

        # 日志输出初始化信息
        self.log(f"[INIT] Initialized DEXLogExtractor with:")
        self.log(f"  - RPC URL: {rpc_url}")
        self.log(f"  - DEX: {dex}")
        self.log(f"  - Pool Address: {pool_address}")
        self.log(f"  - Start Time: {start_time}")
        self.log(f"  - End Time: {end_time}")

    def log(self, message):
        """控制日志输出的函数."""
        if self.enable_logging:
            print(message)

    def datetime_to_block(self, target_datetime):
        """Binary search to find the closest block to a given datetime."""
        target_timestamp = int(target_datetime.timestamp())
        latest_block = self.web3.eth.block_number
        earliest_block = 0

        while earliest_block <= latest_block:
            mid_block = (earliest_block + latest_block) // 2
            mid_timestamp = self.web3.eth.get_block(mid_block)["timestamp"]

            if mid_timestamp < target_timestamp:
                earliest_block = mid_block + 1
            elif mid_timestamp > target_timestamp:
                latest_block = mid_block - 1
            else:
                self.log(f"[BLOCK SEARCH] Exact match found: {mid_block}")
                return mid_block

        self.log(f"[BLOCK SEARCH] Closest block found: {earliest_block}")
        return earliest_block

    def fetch_logs(self):
        """Fetch logs from the specified block range using class-level start_time and end_time."""
        # 获取起始区块号和结束区块号
        start_block = self.datetime_to_block(self.start_time)
        end_block = self.datetime_to_block(self.end_time)
        topic = self.topic_map.get(self.dex)

        self.log(f"[FETCH LOGS] Fetching logs for:")
        self.log(f"  - Pool Address: {self.pool_address}")
        self.log(f"  - Topic: {topic}")
        self.log(f"  - From Block: {start_block}")
        self.log(f"  - To Block: {end_block}")

        # 提取日志
        logs = self.web3.eth.get_logs({
            "topics": [topic],
            "fromBlock": start_block,
            "toBlock": end_block,
            "address": self.pool_address,
        })

        self.log(f"[FETCH LOGS] Retrieved {len(logs)} logs")
        return logs

    def decode_logs(self, logs):
        """Decode logs based on DEX type."""
        decoded_logs = []
        decode_errors = 0
        block_cache = {}  # 缓存区块时间戳，减少重复查询

        for i, log in enumerate(logs):
            #print("data:", log["data"])  # 打印查看数据
            try:
                # 解码日志数据
                #data = binascii.unhexlify(log["data"][2:])
                data = log["data"]
                log_data = {}


                if self.dex == "PancakeSwap_v2":
                    # Pancake V3 ABI types
                    abi_types = ["uint256", "uint256", "uint256", "uint256"]
                    decoded = decode(abi_types, data)
                    log_data = {
                        "transactionHash": log["transactionHash"].hex(),
                        "amount0In": decoded[0] ,
                        "amount1In": decoded[1] ,
                        "amount0Out": decoded[2] ,
                        "amount1Out": decoded[3] ,
                        "sender": log["topics"][1].hex(),
                        "to": log["topics"][2].hex(),
                    }

                elif self.dex == "uniswap_v3":
                    # Uniswap V3 ABI types
                    abi_types = ["int256", "int256", "uint160", "uint128", "int24"]
                    decoded = decode(abi_types, data)
                    log_data = {
                        "transactionHash": log["transactionHash"].hex(),
                        "amount0": decoded[0] ,
                        "amount1": decoded[1] ,
                        "sqrtPriceX96": decoded[2],
                        "liquidity": decoded[3],
                        "tick": decoded[4],
                    }

                elif self.dex == "uniswap_v2":
                    # Uniswap V2 ABI types
                    abi_types = ["uint256", "uint256", "uint256", "uint256"]
                    decoded = decode(abi_types, data)
                    log_data = {
                        "transactionHash": log["transactionHash"].hex(),
                        "amount0In": decoded[0] ,
                        "amount1In": decoded[1] ,
                        "amount0Out": decoded[2] ,
                        "amount1Out": decoded[3] ,
                        "sender": log["topics"][1].hex(),
                        "to": log["topics"][2].hex(),
                    }

                else:
                    print_error(f"Unsupported DEX type: {self.dex}")
                    raise ValueError(f"Unsupported DEX type: {self.dex}")


                # 获取时间戳
                block_number = log.get("blockNumber")
                if not block_number:
                    print_error(f"[DECODE ERROR] Missing blockNumber in log {i + 1}/{len(logs)}")
                    continue

                # 检查缓存是否已有区块时间戳
                if block_number in block_cache:
                    log_data["timestamp"] = block_cache[block_number]
                else:
                    # 查询区块信息获取时间戳
                    try:
                        block = self.web3.eth.get_block(block_number)
                        timestamp = datetime.utcfromtimestamp(block["timestamp"])
                        log_data["timestamp"] = timestamp
                        # 将区块时间戳缓存起来
                        block_cache[block_number] = timestamp
                    except Exception as e:
                        print_error(f"[DECODE ERROR] Failed to fetch block {block_number}: {e}")
                        log_data["timestamp"] = None

                # 将解码后的日志添加到结果中
                decoded_logs.append(log_data)

            except Exception as e:
                decode_errors += 1
                print_error(f"[DECODE ERROR] Error decoding log {i + 1}/{len(logs)}: {e}")

        self.log(f"[DECODE LOGS] Successfully decoded {len(decoded_logs)} logs")
        if decode_errors > 0:
            print_error(f"[DECODE LOGS] Failed to decode {decode_errors} logs")

        return decoded_logs

    def save_to_csv(self, decoded_logs, tokenA_name, tokenB_name,result_dir):
        """Save decoded logs to a CSV file based on DEX type and token addresses."""
        # 检查 decoded_logs 是否为空，如果为空则直接返回
        if not decoded_logs:
            self.log(f"[INFO] No decoded logs to save for {tokenA_name} - {tokenB_name}.")
            return

        # 根据 DEX 和代币生成文件名
        output_file = f"{self.dex}-{tokenA_name}-{tokenB_name}.csv"

        # 保存路径为同目录下的 RESULT 文件夹
        if not os.path.exists(result_dir):  # 如果文件夹不存在，创建它
            os.makedirs(result_dir)

        output_path = os.path.join(result_dir, output_file)

        # 将 decoded_logs 转换为 DataFrame
        new_data = pd.DataFrame(decoded_logs)

        # 检查文件是否已存在
        if os.path.exists(output_path):
            # 如果文件存在，读取现有的数据
            existing_data = pd.read_csv(output_path)

            # 使用 'transactionHash' 列进行去重
            # 保留那些 'transactionHash' 在现有数据中不存在的记录
            new_data = new_data[~new_data['transactionHash'].isin(existing_data['transactionHash'])]

            # 如果没有新的数据，直接返回，不做任何操作
            if new_data.empty:
                self.log(f"[INFO] No new data to add for {tokenA_name} - {tokenB_name}.")
                return

            # 追加新的数据到现有文件
            new_data.to_csv(output_path, mode='a', header=False, index=False)
            self.log(f"[INFO] New logs appended to {output_path}, total {len(new_data)} entries")

        else:
            # 如果文件不存在，直接保存
            new_data.to_csv(output_path, index=False)
            self.log(f"[SAVE TO CSV] Logs saved to {output_path}, total {len(new_data)} entries")


if __name__ == "__main__":
    # 配置参数，直接在程序中赋值
    rpc_url = "https://neat-chaotic-pond.quiknode.pro/13d93177702a33b746afc3218f44d1c8d21679d0"
    dex = "uniswap_v3"
    pool_address = "0x11b815efB8f581194ae79006d24E0d814B7697F6"
    start_time = datetime(2025, 1, 13, 0, 0, 0)
    end_time = datetime(2025, 1, 13,2 ,0 , 30)
    # 示例交易对：指定 tokenA 和 tokenB
    tokenA = "0xdAC17F958D2ee523a2206206994597C13D831ec7"  # USDT
    tokenB = "0xC02aaa39B223Fe8D0A0E5C4F27EAD9083C756Cc2"  # WETH
    tokenAname = "USDT"
    tokenBname = "WETH"

    # 初始化提取器
    extractor = DEXLogExtractor(rpc_url, dex, pool_address, start_time, end_time, enable_logging=True)

    # 提取并解码日志
    logs = extractor.fetch_logs()
    # 保存日志到文件
    #print(logs)
    decoded_logs = extractor.decode_logs(logs)

    # 保存结果到 CSV
    extractor.save_to_csv(decoded_logs,tokenAname,tokenBname,CONFIG["output_path"])
