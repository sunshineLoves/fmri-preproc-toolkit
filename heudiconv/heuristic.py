from __future__ import annotations

import logging
from typing import Optional

from heudiconv.utils import SeqInfo

lgr = logging.getLogger("heudiconv")


def create_key(
    template: Optional[str],
    outtype: tuple[str, ...] = ("nii.gz",),
    annotation_classes: None = None,
) -> tuple[str, tuple[str, ...], None]:
    if template is None or not template:
        raise ValueError("Template must be a valid format string")
    return (template, outtype, annotation_classes)


def infotodict(
    seqinfo: list[SeqInfo],
) -> dict[tuple[str, tuple[str, ...], None], list[str]]:
    """Heuristic evaluator for determining which runs belong where

    allowed template fields - follow python string module:

    item: index within category
    subject: participant id
    seqitem: run number during scanning
    subindex: sub index within group
    """

    # data = create_key("run{item:03d}")
    t1w = create_key("sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:03d}_T1w")
    func_rest = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-rest_run-{item:03d}_bold')
    info: dict[tuple[str, tuple[str, ...], None], list[str]] = {
        t1w: [],
        func_rest: []
    }
    # info: dict[tuple[str, tuple[str, ...], None], list[str]] = {data: []}

    for s in seqinfo:
        """
        The namedtuple `s` contains the following fields:

        * total_files_till_now
        * example_dcm_file
        * series_id
        * dcm_dir_name
        * unspecified2
        * unspecified3
        * dim1
        * dim2
        * dim3
        * dim4
        * TR
        * TE
        * protocol_name
        * is_motion_corrected
        * is_derived
        * patient_id
        * study_description
        * referring_physician_name
        * series_description
        * image_type
        """
        if s.series_description == "MPRAGE":
            info[t1w].append(s.series_id)
        else:
            info[func_rest].append(s.series_id)
    return info
