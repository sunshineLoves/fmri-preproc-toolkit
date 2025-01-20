import os
import argparse
from utils.dispatch import dispatch_container


def get_parser():
    parser = argparse.ArgumentParser()
    # required arguments
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
    parser.add_argument(
        "--fmriprep-output-path",
        type=str,
        required=True,
        help="The directory where the fmriprep output is stored.",
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
    path = os.path.dirname(os.path.abspath(__file__))
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

    parser.add_argument(
        "--license-file",
        type=str,
        default=os.path.join(path, "license.txt"),
        help="The license file name for FreeSurfer.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60 * 30,
        help="The interval time for checking the number of running containers.",
    )
    return parser


def validate_args(args):
    fmriprep_output_path = args.fmriprep_output_path
    bids_dataset_path = args.bids_dataset_path

    os.makedirs(args.docker_log_path, exist_ok=True)
    os.makedirs(args.dispatch_log_path, exist_ok=True)
    assert os.path.isfile(args.license_file), "License file not found."
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
                fmriprep_output_path,
                "sourcedata",
                "freesurfer",
                f"sub-{subject_id}",
            )
        ), f"Output directory of Subject {subject_id}'s sourcedata already exists."
        assert not os.path.exists(
            os.path.join(args.docker_log_path, f"fmriprep_{subject_id}.log")
        ), f"Log file of Subject {subject_id} already exists."

    args.subject_list = subject_list


def main():
    parser = get_parser()
    args = parser.parse_args()
    validate_args(args)
    print(args)

    configs = [{"subject": subject_id} for subject_id in args.subject_list]

    def docker_config_builder(config):
        subject_id = config["subject"]
        return {
            "docker_log_file": f"fmriprep_{subject_id}.log",
            "binds_dict": {
                args.bids_dataset_path: "/data",
                args.fmriprep_output_path: "/out",
                args.license_file: "/opt/freesurfer/license.txt",
            },
            "container_args": [
                "/data",
                "/out",
                "participant",
                "--skip-bids-validation",
                "--output-spaces",
                "MNI152NLin6Asym:res-2",
                "--participant_label",
                subject_id,
                "--nthreads",
                "20",
                "--random-seed",
                "0",
                "--fd-spike-threshold",
                "0.5",
                "--ignore",
                "fieldmaps",
                "--skull-strip-fixed-seed",
                "--stop-on-first-crash",
            ],
            "msg_after_start": f"启动新容器，处理 Subject ID: {subject_id}",
        }

    dispatch_container(
        image_name="nipreps/fmriprep:latest",
        dispatch_log_path=args.dispatch_log_path,
        docker_log_path=args.docker_log_path,
        max_containers=args.max_containers,
        interval=args.interval,
        configs=configs,
        docker_config_builder=docker_config_builder,
    )


if __name__ == "__main__":
    main()
