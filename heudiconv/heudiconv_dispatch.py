import os
import ast
import argparse
from glob import glob
import pandas as pd
from utils.dispatch import dispatch_container


def get_parser():
    parser = argparse.ArgumentParser()
    # required arguments
    parser.add_argument(
        "--adni-raw-path",
        type=str,
        required=True,
        help="The directory where the ADNI raw dataset is located.",
    )
    parser.add_argument(
        "--bids-output-path",
        type=str,
        required=True,
        help="The directory where the BIDS output is stored.",
    )
    parser.add_argument(
        "--image-info-csv",
        type=str,
        required=True,
        help="The CSV file containing the image information.",
    )
    # arguments for dispatching container
    parser.add_argument(
        "--max-containers",
        type=int,
        required=True,
        help="The maximum number of containers that can be run simultaneously.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        required=True,
        help="The interval time for checking the number of running containers.",
    )
    # optional arguments
    path = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument(
        "--heuristic-file",
        type=str,
        default=os.path.join(path, "heuristic.py"),
        help="The heuristic file for heudiconv.",
    )
    parser.add_argument(
        "--docker-log-path",
        type=str,
        default=os.path.join(path, "docker-logs"),
        help="The directory where the docker logs are stored.",
    )
    parser.add_argument(
        "--dispatch-log-path",
        type=str,
        default=os.path.join(path, "dispatch-logs"),
        help="The directory where the dispatch logs are stored.",
    )
    return parser


def validate_args(args):
    assert os.path.isdir(args.adni_raw_path), "ADNI raw dataset not found."
    assert os.path.isfile(args.image_info_csv), "Image info CSV file not found."
    assert os.path.isfile(args.heuristic_file), "Heuristic file not found."
    assert not os.path.exists(args.bids_output_path) or os.path.isdir(
        args.bids_output_path
    ), "BIDS output path is not a directory."

    os.makedirs(args.docker_log_path, exist_ok=True)
    os.makedirs(args.dispatch_log_path, exist_ok=True)


def get_binds_dict(adni_raw_path, prefix, image_ids):
    find_image_path = lambda ID: glob(f"{adni_raw_path}\\*\\*\\*\\I{ID}")[0]
    get_bind_path = lambda ID: f"{prefix}/I{ID}"
    return dict(zip(map(find_image_path, image_ids), map(get_bind_path, image_ids)))


def heudiconv_main(argv=None):
    parser = get_parser()
    args = parser.parse_args(argv)
    validate_args(args)
    print(args)

    images_df = pd.read_csv(
        args.image_info_csv,
        index_col=[0, 1],
        converters={"fmri_images": ast.literal_eval, "mri_images": ast.literal_eval},
    )
    configs = []
    for subject_id, viscode in images_df.index:
        item = images_df.loc[(subject_id, viscode)]
        fmri_images = item.fmri_images
        mri_images = item.mri_images
        configs.append(
            {
                "subject_id": subject_id,
                "viscode": viscode,
                "fmri_images": fmri_images,
                "mri_images": mri_images,
            }
        )

    def docker_config_action(_, config):
        subject_id = config["subject_id"]
        viscode = config["viscode"]
        fmri_images = config["fmri_images"]
        mri_images = config["mri_images"]
        return {
            "docker_log_file": f"heudiconv_{subject_id}_{viscode}.log",
            "binds_dict": {
                # The heuristic file is mounted to the container
                args.heuristic_file: "/data/heuristic.py",
                # The ADNI raw dataset is mounted to the container
                **get_binds_dict(
                    args.adni_raw_path,
                    f"/data/{subject_id}/{viscode}",
                    fmri_images + mri_images,
                ),
                # The BIDS output directory is mounted to the container
                args.bids_output_path: "/out",
            },
            "container_args": [
                "-d",
                "/data/{subject}/{session}/*/*.dcm",
                "-o",
                "/out",
                "-f",
                "/data/heuristic.py",
                "-c",
                "dcm2niix",
                "-s",
                subject_id,
                "-ss",
                viscode,
                "-b",
                "--overwrite",
            ],
            "msg_after_start": f"启动新容器，转换 Subject：{subject_id}，Visit：{viscode}",
        }

    dispatch_container(
        image_name="nipy/heudiconv:latest",
        dispatch_log_path=args.dispatch_log_path,
        docker_log_path=args.docker_log_path,
        max_containers=args.max_containers,
        interval=args.interval,
        configs=configs,
        docker_config_action=docker_config_action,
    )


if __name__ == "__main__":
    heudiconv_main()
