import os
import subprocess
import time
from threading import Thread, Lock
from datetime import datetime
import argparse


def dispatch_fmriprep_container(
    bids_dataset_path,
    max_containers,
    docker_log_path,
    dispatch_log_path,
    fmriprep_output_path,
    license_file,
    subject_list,
    interval,
):
    print_lock = Lock()
    file_lock = Lock()

    def log_message(message):
        """打印带有时间戳的日志信息"""
        log = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
        with print_lock:
            print(log)
        # 同时将日志信息写入文件
        with file_lock:
            with open(dispatch_log_file, "a") as f:
                f.write(log + "\n")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dispatch_log_file = os.path.join(dispatch_log_path, f"dispatch_{timestamp}.log")
    log_message(
        f"处理的受试者总数为: {len(subject_list)}，受试者标签列表为：{subject_list}"
    )

    def wait_container(container_id, docker_log_file):
        """启动容器并传递数据路径作为环境变量"""
        log_message(f"等待容器退出...")
        exit_code = (
            subprocess.check_output(["docker", "wait", container_id]).decode().strip()
        )
        log_message(f"容器退出码为 {exit_code}，日志记录到 {docker_log_file}")
        with open(docker_log_file, "w") as f:
            subprocess.run(["docker", "logs", "-t", container_id], stdout=f, stderr=f)
        log_message("删除容器")
        subprocess.check_call(
            ["docker", "rm", container_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    for subject_id in subject_list:
        docker_log_file = os.path.join(docker_log_path, f"fmriprep_{subject_id}.log")

        command = [
            "docker",
            "run",
            "-d",
            "-v",
            f"{bids_dataset_path}:/data",
            "-v",
            f"{fmriprep_output_path}:/out",
            "-v",
            f"{license_file}:/opt/freesurfer/license.txt",
            "nipreps/fmriprep:latest",
            "/data",
            "/out",
            "participant",
            "--output-spaces",
            "MNI152NLin2009cAsym:res-1",
            "MNI152NLin6Asym:res-1",
            "--participant_label",
            subject_id,
            "--ignore",
            "fieldmaps",
        ]

        """监控容器数量并启动新容器"""
        while True:
            # 获取当前运行的容器数量
            running_containers = int(
                subprocess.check_output(["docker", "ps", "-q"]).decode().count("\n")
            )

            # 如果容器数量少于最大容器数，则启动新的容器
            if running_containers < max_containers:
                # 启动容器
                log_message(f"启动新容器，处理 Subject ID: {subject_id}")
                container_id = subprocess.check_output(command).decode().strip()
                Thread(
                    target=wait_container,
                    args=(container_id, docker_log_file),
                ).start()
                break

            # 定期检查
            log_message(f"当前正在运行的容器数量: {running_containers}")
            time.sleep(interval)  # 可调节时间间隔，平衡系统负载与实时性
    log_message("处理完成")


def get_parser():
    parser = argparse.ArgumentParser()
    # required arguments
    parser.add_argument(
        "--toolkit-path",
        type=str,
        required=True,
        help="The directory where the fMRI-preproc-toolkit is located.",
    )
    parser.add_argument(
        "--bids-dataset-path",
        type=str,
        required=True,
        help="The directory where the BIDS dataset is located.",
    )
    parser.add_argument(
        "--max-containers",
        type=int,
        required=True,
        help="The maximum number of containers that can be run simultaneously.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--subject-list",
        type=str,
        nargs="+",
        help="The list of subject IDs to be processed.",
    )
    group.add_argument(
        "--subject-range",
        type=str,
        nargs=2,
        help="The range of subject IDs to be processed.",
    )
    group.add_argument(
        "--subject-file",
        type=str,
        help="the file containing the list of subject IDs to be processed.",
    )
    # optional arguments
    parser.add_argument(
        "--workspace-name",
        type=str,
        default="fmriprep",
        help="The name of the fmriprep workspace directory.",
    )
    parser.add_argument(
        "--docker-log-path",
        type=str,
        default="docker-logs",
        help="The directory where the docker logs are stored. (relative to --workspace-name)",
    )
    parser.add_argument(
        "--dispatch-log-path",
        type=str,
        default="dispatch-logs",
        help="The directory where the dispatch logs are stored. (relative to --workspace-name)",
    )
    parser.add_argument(
        "--fmriprep-output-path",
        type=str,
        default="output",
        help="The directory where the fmriprep output is stored. (relative to --workspace-name)",
    )
    parser.add_argument(
        "--license-file",
        type=str,
        default="license.txt",
        help="The license file name for FreeSurfer. (relative to --workspace-name)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60 * 30,
        help="The interval time for checking the number of running containers.",
    )
    return parser


def validate_args(args):
    workspace_path = os.path.join(args.toolkit_path, args.workspace_name)
    bids_dataset_path = args.bids_dataset_path
    docker_log_path = os.path.join(workspace_path, args.docker_log_path)
    dispatch_log_path = os.path.join(workspace_path, args.dispatch_log_path)
    fmriprep_output_path = os.path.join(workspace_path, args.fmriprep_output_path)
    license_file = os.path.join(workspace_path, args.license_file)

    os.makedirs(docker_log_path, exist_ok=True)
    os.makedirs(dispatch_log_path, exist_ok=True)
    assert os.path.isfile(license_file), "License file not found."
    assert os.path.isdir(bids_dataset_path), "BIDS dataset not found."
    assert not os.path.exists(fmriprep_output_path) or os.path.isdir(
        fmriprep_output_path
    ), "fmriprep output path is not a directory."

    subject_list = []
    if args.subject_list:
        for subject_id in args.subject_list:
            assert os.path.isdir(
                os.path.join(bids_dataset_path, f"sub-{subject_id}")
            ), f"Subject {subject_id} not found."
        subject_list = args.subject_list
    elif args.subject_range:
        subject_paths = sorted(os.listdir(bids_dataset_path))
        subject_ids = [path.split("-")[-1] for path in subject_paths]
        start, end = args.subject_range
        start_i = subject_ids.index(start)
        end_i = subject_ids.index(end)
        assert start_i < end_i and start_i >= 0 and end_i >= 0, "Invalid subject range."
        subject_list = subject_ids[start_i : end_i + 1]
    elif args.subject_file:
        with open(args.subject_file, "r") as f:
            for line in f.readlines():
                subject_id = line.strip()
                assert os.path.isdir(
                    os.path.join(bids_dataset_path, f"sub-{subject_id}")
                ), f"Subject {subject_id} not found."
                subject_list.append(subject_id)
    for subject_id in subject_list:
        assert not os.path.exists(
            os.path.join(fmriprep_output_path, f"sub-{subject_id}")
        ), f"Output directory of Subject {subject_id} already exists."
        assert not os.path.exists(
            os.path.join(
                fmriprep_output_path, "sourcedata", "freesurfer", f"sub-{subject_id}"
            )
        ), f"Output directory of Subject {subject_id}'s sourcedata already exists."
        assert not os.path.exists(
            os.path.join(docker_log_path, f"fmriprep_{subject_id}.log")
        ), f"Log file of Subject {subject_id} already exists."

    args.docker_log_path = docker_log_path
    args.dispatch_log_path = dispatch_log_path
    args.fmriprep_output_path = fmriprep_output_path
    args.license_file = license_file
    args.subject_list = subject_list


def main():
    parser = get_parser()
    args = parser.parse_args()
    validate_args(args)
    print(args)

    dispatch_fmriprep_container(
        args.bids_dataset_path,
        args.max_containers,
        args.docker_log_path,
        args.dispatch_log_path,
        args.fmriprep_output_path,
        args.license_file,
        args.subject_list,
        args.interval,
    )


if __name__ == "__main__":
    main()
