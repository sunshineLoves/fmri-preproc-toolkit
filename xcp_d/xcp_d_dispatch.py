import os
import argparse
from utils.dispatch import dispatch_container


def get_parser():
    parser = argparse.ArgumentParser()
    # required arguments
    parser.add_argument(
        "--fmriprep-derivative-path",
        type=str,
        required=True,
        help="The directory where the fMRIPrep derivatives is located.",
    )
    parser.add_argument(
        "--bids-atlas-path",
        type=str,
        required=True,
        help="The directory where the BIDS Atlases Dataset is located.",
    )
    parser.add_argument(
        "--xcp_d-output-path",
        type=str,
        required=True,
        help="The directory where the xcp_d output is stored.",
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
    # arguments for dispatching container
    parser.add_argument(
        "--max-containers",
        type=int,
        required=True,
        help="The maximum number of containers that can be run simultaneously.",
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
    return parser


def validate_args(args):
    fmriprep_derivative_path = args.fmriprep_derivative_path
    # docker_log_path = os.path.join(workspace_path, args.docker_log_path)
    # dispatch_log_path = os.path.join(workspace_path, args.dispatch_log_path)
    xcp_d_output_path = args.xcp_d_output_path

    os.makedirs(args.docker_log_path, exist_ok=True)
    os.makedirs(args.dispatch_log_path, exist_ok=True)
    assert os.path.isdir(fmriprep_derivative_path), "fMRIPrep output not found."
    assert not os.path.exists(xcp_d_output_path) or os.path.isdir(
        xcp_d_output_path
    ), "xcp_d output path is not a directory."

    subject_list = []
    if args.subject_list:
        for subject_id in args.subject_list:
            assert os.path.isdir(
                os.path.join(fmriprep_derivative_path, f"sub-{subject_id}")
            ), f"Subject {subject_id} not found."
        subject_list = args.subject_list
    elif args.subject_range:
        paths = sorted(
            [
                path
                for path in os.listdir(fmriprep_derivative_path)
                if os.path.isdir(os.path.join(fmriprep_derivative_path, path))
                and path.startswith("sub-")
            ]
        )
        subject_ids = [path.split("-")[-1] for path in paths]
        start, end = args.subject_range
        start_i = subject_ids.index(start)
        end_i = subject_ids.index(end)
        assert (
            start_i <= end_i and start_i >= 0 and end_i >= 0
        ), "Invalid subject range."
        subject_list = subject_ids[start_i : end_i + 1]
    elif args.subject_file:
        with open(args.subject_file, "r") as f:
            for line in f.readlines():
                subject_id = line.strip()
                assert os.path.isdir(
                    os.path.join(fmriprep_derivative_path, f"sub-{subject_id}")
                ), f"Subject {subject_id} not found."
                subject_list.append(subject_id)
    for subject_id in subject_list:
        assert not os.path.exists(
            os.path.join(xcp_d_output_path, f"sub-{subject_id}")
        ), f"Output directory of Subject {subject_id} already exists."
        assert not os.path.exists(
            os.path.join(args.docker_log_path, f"xcp_d_{subject_id}.log")
        ), f"Log file of Subject {subject_id} already exists."

    args.subject_list = subject_list


def xcp_d_main(argv=None):
    parser = get_parser()
    args = parser.parse_args(argv)
    validate_args(args)
    print(args)

    configs = [{"subject_id": subject_id} for subject_id in args.subject_list]

    def docker_config_action(_, config):
        subject_id = config["subject_id"]
        return {
            "docker_log_file": f"xcp_d_{subject_id}.log",
            "binds_dict": {
                args.fmriprep_derivative_path: "/fmri_dir",
                args.xcp_d_output_path: "/output_dir",
                args.bids_atlas_path: "/bids-atlas",
            },
            "container_args": [
                "/fmri_dir",
                "/output_dir",
                "participant",
                "--mode",
                "abcd",
                "--participant_label",
                subject_id,
                "--nthreads",
                "12",
                "--input-type",
                "fmriprep",
                "--file-format",
                "nifti",
                "--nuisance-regressors",
                "36P",
                "--smoothing",
                "6",
                "--lower-bpf",
                "0.009",
                "--upper-bpf",
                "0.08",
                "--motion-filter-type",
                "none",
                "--head-radius",
                "auto",
                "--fd-thresh",
                "0.2",
                "--min-time",
                "0",
                "--datasets",
                "/bids-atlas",
                "--atlases",
                "AAL424",
                "Schaefer400Tian450",
                "--min-coverage",
                "0.0",
                "--create-matrices",
                "all",
                "--random-seed",
                "0",
                "--warp-surfaces-native2std",
                "n",
                "--stop-on-first-crash",
            ],
            "msg_after_start": f"启动新容器，处理 Subject ID: {subject_id}",
        }

    dispatch_container(
        image_name="pennlinc/xcp_d:latest",
        dispatch_log_path=args.dispatch_log_path,
        docker_log_path=args.docker_log_path,
        max_containers=args.max_containers,
        configs=configs,
        docker_config_action=docker_config_action,
    )


if __name__ == "__main__":
    xcp_d_main()
