import argparse

from fmriprep.fmriprep_dispatch import fmriprep_main
from heudiconv.heudiconv_dispatch import heudiconv_main
from xcp_d.xcp_d_dispatch import xcp_d_main


def main():
    parser = argparse.ArgumentParser(
        description="Run a specific task with its arguments.", add_help=False
    )
    parser.add_argument(
        "task",
        choices=["fmriprep", "heudiconv", "xcp_d"],
        help="The name of the task to run.",
    )
    # 使用 parse_known_args 来允许未知参数的存在
    args, unknown = parser.parse_known_args()

    if args.task == "fmriprep":
        fmriprep_main(unknown)
    elif args.task == "heudiconv":
        heudiconv_main(unknown)
    elif args.task == "xcp_d":
        xcp_d_main(unknown)
    else:
        raise ValueError(f"Unknown script name: {args.task}")


if __name__ == "__main__":
    main()
