import os
import nibabel as nib
from tqdm import tqdm
import numpy as np


def convert_fMRIvols_to_atlas(fmri_files, atlas_file):
    # Where is the data located?
    fmri_count = len(fmri_files)
    print("Number of fMRI files:", fmri_count)
    print("Atlas file:", atlas_file)
    try:
        label_img = nib.load(atlas_file)
        atlas_shape = label_img.shape
        label = label_img.get_fdata()
        label = label.flatten()
        unique_labels = np.unique(label)
        nParcels = len(unique_labels) - 1
        assert np.array_equal(
            unique_labels.astype(np.int64), np.arange(nParcels + 1)
        ), f"Labels must be integers from 0 to {nParcels}"
        print("Atlas successfully loaded")
        parcel_to_indices = {i: np.where(label == i + 1)[0] for i in range(0, nParcels)}
    except Exception as e:
        print(f"Loading dlabel Atlas File Error for {atlas_file}: {str(e)}")

    pmTS_list = []
    for index, fmri_file in enumerate(fmri_files):
        # Load images and labels
        if ".nii.gz" in fmri_file:
            file_name = os.path.basename(fmri_file)
            print(f"Processing {index + 1}/{fmri_count} {file_name}")
            print(f"Loading 4D image from {fmri_file}")

            try:
                dts_img = nib.load(fmri_file)
                dts_shape = dts_img.shape
                assert dts_shape[:-1] == atlas_shape, "fMRI and Atlas shape mismatch"
                dts = dts_img.get_fdata()
                print("Loaded fMRI data")
            except Exception as e:
                print(f"Loading 4D File Error for {file_name}: {str(e)}")

            try:
                print(f"Extracting Parcels for {file_name}")
                m = dts.reshape((-1, dts.shape[-1])).T
                sh = m.shape[0]

                # Get parcellated time series given a label input
                # print(f'\nGet parcellated time series using {l}\n\n')
                pmTS = np.zeros((sh, nParcels))

                items = parcel_to_indices.items()
                for i, indices in tqdm(items, desc="Extracting parcels", unit="parcel"):
                    pmTS[:, i] = np.nanmean(m[:, indices], axis=1)

                # Replace NaNs with 0
                pmTS[np.isnan(pmTS)] = 0
                pmTS_list.append(pmTS)

            except:
                print(f"Error with parcel Extraction for {file_name}")

        else:
            print(f"File {file_name} not a nifti file. Skipping...")

    return pmTS_list
