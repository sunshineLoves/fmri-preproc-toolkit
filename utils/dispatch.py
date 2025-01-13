import os
import subprocess
import time
from threading import Thread, Lock
from datetime import datetime
from typing import List, Dict, Callable


def dispatch_container(
    image_name: str,
    dispatch_log_path: str,
    docker_log_path: str,
    max_containers: int,
    interval: int,
    configs: List[Dict[str, str]],
    docker_config_builder: Callable[[Dict[str, str]], Dict[str, str]],
):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filtered_name = image_name.replace("/", "-").replace(":", "-")
    dispatch_log_file = os.path.join(
        dispatch_log_path, f"dispatch_{filtered_name}_{timestamp}.log"
    )

    print_lock = Lock()
    file_lock = Lock()
    counter_lock = Lock()

    exit_code_counter = {}

    def log(message):
        """打印带有时间戳的日志信息"""
        msg = f"[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] {message}"
        with print_lock:
            print(msg)
        # 同时将日志信息写入文件
        with file_lock:
            with open(dispatch_log_file, "a") as f:
                f.write(msg + "\n")

    log(f"开始调度容器，容器镜像为：{image_name}，计划运行的容器个数: {len(configs)}")

    def wait_container(container_id, docker_log_file):
        log(f"等待容器 {container_id} 退出...")
        exit_code = (
            subprocess.check_output(["docker", "wait", container_id]).decode().strip()
        )
        with counter_lock:
            if exit_code in exit_code_counter:
                exit_code_counter[exit_code] += 1
            else:
                exit_code_counter[exit_code] = 1
        log(f"容器 {container_id} 退出码为 {exit_code}，日志记录到 {docker_log_file}")
        with open(docker_log_file, "w") as f:
            subprocess.run(["docker", "logs", "-t", container_id], stdout=f, stderr=f)
        log(f"删除容器 {container_id}...")
        subprocess.check_call(
            ["docker", "rm", container_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log(f"容器 {container_id} 删除成功")

    for config in configs:
        docker_config = docker_config_builder(config)
        docker_log_file = os.path.join(
            docker_log_path, docker_config["docker_log_file"]
        )
        binds_dict = docker_config["binds_dict"]
        bind_args = []
        for src, dst in binds_dict.items():
            bind_args.extend(["-v", f"{src}:{dst}"])
        container_args = docker_config["container_args"]
        command = ["docker", "run", "-d", *bind_args, image_name, *container_args]

        """监控容器数量并启动新容器"""
        while True:
            # 获取当前运行的容器数量
            running_containers = int(
                subprocess.check_output(["docker", "ps", "-q"]).decode().count("\n")
            )

            # 如果容器数量少于最大容器数，则启动新的容器
            if running_containers < max_containers:
                # 启动容器
                log(f"启动新容器...")
                log(f"参数配置 : {config}")
                log(f"路径绑定 : {binds_dict}")
                log(f"容器参数 : {" ".join(container_args)}")
                container_id = subprocess.check_output(command).decode().strip()
                log(f"启动成功，容器 ID: {container_id}")
                log(docker_config["msg_before_start"])
                Thread(
                    target=wait_container,
                    args=(container_id, docker_log_file),
                ).start()
                break

            # 定期检查
            log(f"当前正在运行的容器数量: {running_containers}")
            time.sleep(interval)  # 可调节时间间隔，平衡系统负载与实时性
    log("处理完成")
    log(f"容器退出码统计: {exit_code_counter}")
