from datetime import datetime
import pandas as pd
import os
from .config import CONFIG
from .Calculator import Calculator
from .DEXLogExtractor import DEXLogExtractor
from .PoolAddressSearcher import PoolAddressSearcher

class ETHfetch:
    def __init__(self, start_time, end_time, interval):
        self.rpc_url = CONFIG["rpc_url"]
        self.input_csv1 = CONFIG["input_csv1"]
        self.input_csv2 = CONFIG["input_csv2"]
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval
        self.enable_logging = CONFIG["enable_logging"]
        self.output_path = CONFIG["output_path"]  # 从配置文件读取输出路径
        self.factory_df = pd.read_csv(self.input_csv1)
        self.pair_df = pd.read_csv(self.input_csv2)

    def print_error(self, message):
        # 红色的 ANSI 转义字符代码是 31
        print(f"\033[31m{message}\033[0m")  # 31 是红色，0 是重置颜色

    def eth_fetch(self):
        results = []
        success_count = 0
        failure_count = 0

        print("[INFO] PoolAddress Searcher Started")
        for factory_index, factory_row in self.factory_df.iterrows():
            dex = factory_row["dex"]
            factory_address = factory_row["factoryaddress"]
            factory_abi = eval(factory_row["factoryabi"])
            function_call = factory_row["getfuction"]

            for pair_index, pair_row in self.pair_df.iterrows():
                tokenA = pair_row["tokenA"]
                tokenB = pair_row["tokenB"]
                tokenAname = pair_row["tokenAname"]
                tokenBname = pair_row["tokenBname"]

                # 查询池地址
                searcher = PoolAddressSearcher(
                    self.rpc_url, tokenA, tokenB, dex, factory_address, factory_abi, function_call, self.enable_logging
                )

                pool_address = searcher.query_pool_address()

                if pool_address and pool_address.strip():
                    success_count += 1
                    results.append({
                        "dex": dex,
                        "factory_address": factory_address,
                        "tokenA": tokenA,
                        "tokenAname": tokenAname,
                        "tokenB": tokenB,
                        "tokenBname": tokenBname,
                        "pool_address": pool_address
                    })
                else:
                    failure_count += 1
                    self.print_error(f"[WARNING] Skipping pair (dex：{dex}，TokenA: {tokenA}, TokenB: {tokenB}) due to empty pool address.")

        results_df = pd.DataFrame(results)
        # 修改输出路径为从配置文件读取
        output_csv_path = os.path.join(self.output_path, "search_pooladdr_bypair.csv")
        results_df.to_csv(output_csv_path, index=False)
        print(f"[INFO] PoolAddress Search completed: {success_count} records succeeded, {failure_count} records failed.")

        print("[INFO] Log Fetch and Decoding...")
        data = pd.read_csv(output_csv_path)
        for _, row in data.iterrows():
            dex = row["dex"]
            pool_address = row["pool_address"]
            tokenA = row["tokenA"]
            tokenAname = row["tokenAname"]
            tokenB = row["tokenB"]
            tokenBname = row["tokenBname"]

            if pool_address is None or pool_address == "":
                if self.enable_logging:
                    self.print_error(f"[WARNING] Skipping row {tokenAname}-{tokenBname}-{pool_address} due to missing pool address.")
                continue

            extractor = DEXLogExtractor(
                rpc_url=self.rpc_url,
                dex=dex,
                pool_address=pool_address,
                start_time=self.start_time,
                end_time=self.end_time,
                enable_logging=self.enable_logging
            )
            logs = extractor.fetch_logs()
            decoded_logs = extractor.decode_logs(logs)
            extractor.save_to_csv(decoded_logs, tokenAname, tokenBname, self.output_path)  # 修改输出路径
        print("[INFO] Log Fetch and Decoding Completed")

        print("[INFO] Start Calculating...")
        for _, row in data.iterrows():
            calculator = Calculator(
                rpc_url=self.rpc_url,
                pooladdress=row["pool_address"],
                tokenA=row["tokenA"],
                tokenAname=row["tokenAname"],
                tokenB=row["tokenB"],
                tokenBname=row["tokenBname"],
                dex=row["dex"],
                interval=self.interval,
                enable_logging=self.enable_logging
            )
            calculator.calculate()
        print("[INFO] Calculate Completed")

if __name__ == "__main__":
    start_time = datetime(2025, 1, 13, 0, 0, 0)
    end_time = datetime(2025, 1, 13, 2, 0, 0)
    interval = "5min"
    analyzer = ETHfetch(start_time, end_time, interval)
    analyzer.eth_fetch()
