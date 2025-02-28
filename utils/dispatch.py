import os
import docker
from datetime import datetime
from typing import List, Dict, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


def dispatch_container(
    image_name: str,
    dispatch_log_path: str,
    docker_log_path: str,
    max_containers: int,
    configs: List[Dict[str, str]],
    docker_config_action: Callable[[str, Dict[str, str]], Dict[str, str]],
    workdirs_path: str = None,
):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    docker_log_path = os.path.join(docker_log_path, timestamp)
    filtered_name = image_name.replace("/", "-").replace(":", "-")
    dispatch_log_file = os.path.join(
        dispatch_log_path, f"dispatch_{filtered_name}_{timestamp}.log"
    )

    os.makedirs(docker_log_path, exist_ok=True)
    if workdirs_path:
        workdirs_path = os.path.join(workdirs_path, timestamp)
        os.makedirs(workdirs_path, exist_ok=True)

    log_lock = Lock()

    def log(*args):
        """打印带有时间戳的日志信息"""
        with log_lock:
            for msg in args:
                format_msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
                print(format_msg)
                # 同时将日志信息写入文件
                with open(dispatch_log_file, "a") as f:
                    f.write(format_msg + "\n")

    count = len(configs)
    log(f"开始调度容器，容器镜像为：{image_name}，计划运行的容器个数: {count}")

    def run_container(index, config, docker_log_file):
        client = docker.from_env()
        docker_config = docker_config_action(workdirs_path, config)
        docker_log_file = os.path.join(
            docker_log_path, docker_config["docker_log_file"]
        )
        binds_dict = docker_config["binds_dict"]
        bind_args = [f"{src}:{dst}" for src, dst in binds_dict.items()]
        container_args = docker_config["container_args"]
        log(
            f"开始启动第 {index + 1} / {count} 个容器...",
            f"参数配置 : {config}",
            f"路径绑定 : {binds_dict}",
            f"容器参数 : {' '.join(container_args)}",
        )
        container = client.containers.run(
            image=image_name,
            detach=True,
            volumes=bind_args,
            command=container_args,
        )
        container_name = container.name
        log(
            f"启动第 {index + 1} / {count} 个容器成功，容器名称: {container_name}",
            docker_config["msg_after_start"],
            f"等待容器 {container_name} 退出...",
        )
        exit_code = container.wait()["StatusCode"]
        log(
            f"第 {index + 1} / {count} 个容器 {container_name} 退出码为 {exit_code}",
            f"容器参数配置为 {config}，日志记录到 {docker_log_file}",
        )
        with open(docker_log_file, "w") as f:
            f.write(container.logs(timestamps=True).decode())
        log(f"删除容器 {container_name}...")
        container.remove()
        log(f"容器 {container_name} 删除成功")
        return exit_code

    exit_code_counter = {}
    exit_code_infos = []
    with ThreadPoolExecutor(max_workers=max_containers) as executor:
        futures = {
            executor.submit(run_container, index, config, docker_log_path): index
            for index, config in enumerate(configs)
        }
        log("所有容器已提交，等待处理完成...")
        for future in as_completed(futures):
            # 获取完成的任务的结果
            index = futures[future]
            config = configs[index]
            try:
                exit_code = future.result()  # 任务的返回值
                exit_code_infos.append({"config": config, "exit_code": exit_code})
                if exit_code in exit_code_counter:
                    exit_code_counter[exit_code] += 1
                else:
                    exit_code_counter[exit_code] = 1
            except Exception as exc:
                log(f"第 {index + 1} / {count} 个容器 {config} 处理出现异常: {exc}")

    log(
        "处理完成",
        f"容器退出码信息: {exit_code_infos}",
        f"容器退出码统计: {exit_code_counter}",
    )
