# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""OAISTree directory structure."""
import csv
import os
import shutil
from datetime import datetime
from tempfile import NamedTemporaryFile

from pyDataverse.models import Datafile, Dataset
from pyDataverse.utils import read_json, write_file, write_json

SIP_FOLDER = "SIP"
AIP_FOLDER = "AIP"
DIP_FOLDER = "DIP"


def setup_oaistree(
    dataset_dir, dataset_id, terms_of_use, terms_of_access, overwrite_all=False
):
    """Set up an empty DVTree directory structure.

    Parameters
    ----------
    dataset_dir : string
        Full path of directory, where Dataset directory structure should be
        created in.

    """
    terms_of_use_filename = "terms-of-use.html"
    terms_of_access_filename = "terms-of-access.html"
    if not os.path.isdir(dataset_dir):
        os.mkdir(dataset_dir)
    if not os.path.isdir(os.path.join(dataset_dir, SIP_FOLDER)):
        os.mkdir(os.path.join(dataset_dir, SIP_FOLDER))
    if not os.path.isdir(os.path.join(dataset_dir, AIP_FOLDER)):
        os.mkdir(os.path.join(dataset_dir, AIP_FOLDER))
    if not os.path.isdir(os.path.join(dataset_dir, DIP_FOLDER)):
        os.mkdir(os.path.join(dataset_dir, DIP_FOLDER))
    if (
        not os.path.isfile(os.path.join(dataset_dir, DIP_FOLDER, terms_of_use_filename))
        or overwrite_all
    ):
        write_file(
            os.path.join(dataset_dir, DIP_FOLDER, terms_of_use_filename), terms_of_use
        )
    if (
        not os.path.isfile(
            os.path.join(dataset_dir, DIP_FOLDER, terms_of_access_filename)
        )
        or overwrite_all
    ):
        write_file(
            os.path.join(dataset_dir, DIP_FOLDER, terms_of_access_filename),
            terms_of_access,
        )
    if overwrite_all:
        history = {}
        history["creation_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history["dataset_id"] = dataset_id
        history["dataset_foldername"] = dataset_dir.split("/")[-1]
    else:
        if os.path.isfile(
            os.path.join(dataset_dir, "{0}_history.json".format(dataset_id))
        ):
            history = read_json(
                os.path.join(dataset_dir, "{0}_history.json".format(dataset_id))
            )
        else:
            history = {}
            history["creation_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history["dataset_id"] = dataset_id
            history["dataset_foldername"] = dataset_dir.split("/")[-1]
    save_history(dataset_dir, history)


def delete_all_folders_inside(base_dir):
    """Delete all child-directories inside a directory.

    Parameters
    ----------
    base_dir : string
        Full path of base directory.

    """
    [
        shutil.rmtree(os.path.join(base_dir, dI))
        for dI in os.listdir(base_dir)
        if os.path.isdir(os.path.join(base_dir, dI))
    ]


def datafile_from_raw_to_sip(filename_source, filename_target, overwrite_all=False):
    """Save raw data to SIP.

    Parameters
    ----------
    filename_source : string
        Relative path of source file.
    filename_target : string
        Relative path of target file.

    """
    if not os.path.isfile(filename_target) or overwrite_all:
        shutil.copyfile(filename_source, filename_target)


def datafile_from_sip_to_aip(filename_source, filename_target, overwrite_all=False):
    """Process raw files from SIP to AIP.

    Parameters
    ----------
    dataset_dir : string
        Full path of dataset directory.
    filename_source : string
        Relative path of source file.
    filename_target : string
        Relative path of target file.

    """
    if not os.path.isfile(filename_target) or overwrite_all:
        shutil.copyfile(filename_source, filename_target)
    # os.path.join(dataset_dir, AIP_FOLDER, '{0}'.format(filename_target)))


def datafile_from_aip_to_dip(dataset_dir, filename, overwrite_all=False):
    """Short summary.

    Parameters
    ----------
    dataset_dir : string
        Full path of dataset directory.
    filename : string
        Relative path of file.

    """
    if (
        not os.path.isfile(
            os.path.join(dataset_dir, DIP_FOLDER, "{0}".format(filename))
        )
        or overwrite_all
    ):
        shutil.copyfile(
            os.path.join(dataset_dir, AIP_FOLDER, "{0}".format(filename)),
            os.path.join(dataset_dir, DIP_FOLDER, "{0}".format(filename)),
        )


def save_dataset_dataverse_json(dataset_dir, data, dataset_id):
    """Save dataset JSON to DVTree structure.

    Parameters
    ----------
    dataset_dir : string
        Full path of dataset directory.
    data : dict
        Dataset data as dict.
    dataset_id : string
        Dataset ID.

    """
    ds = Dataset()
    ds.set(data)
    write_file(
        os.path.join(dataset_dir, AIP_FOLDER, "{0}_dataset.json".format(dataset_id)),
        ds.json(),
    )


def save_datafile_dataverse_json(dataset_dir, data, dataset_id, datafile_id):
    """Save datafile JSON to DVTree structure.

    Parameters
    ----------
    dataset_dir : string
        Full path of dataset directory.
    data : dict
        Datafile data as dict.
    dataset_id : string
        Dataset ID.
    datafile_id : string
        Datafile ID.

    """
    datafile = Datafile()
    datafile.set(data)
    datafile.export_data(
        os.path.join(
            dataset_dir,
            AIP_FOLDER,
            "{0}_{1}_datafile.json".format(dataset_id, datafile_id),
        )
    )


def read_history(dataset_dir):
    """Read internal Dataset metadata from JSON file.

    Parameters
    ----------
    dataset_dir : string
        Full path of dataset directory.

    Returns
    -------
    dict
        Internal Dataset related history as dict.

    """
    dataset_id = dataset_dir.split("/")[-1]
    history = read_json(
        os.path.join(dataset_dir, "{0}_history.json".format(dataset_id))
    )
    return history


def save_history(dataset_dir, history):
    """Short summary.

    Parameters
    ----------
    dataset_dir : type
        Description of parameter `dataset_dir`.
    history : type
        Description of parameter `history`.

    Returns
    -------
    type
        Description of returned object.

    Raises
    -------
    ExceptionName
        Why the exception is raised.

    Examples
    -------
    Examples should be written in doctest format, and
    should illustrate how to use the function/class.
    >>>

    """
    """Save internal Dataset metadata to JSON file.

    Parameters
    ----------
    dataset_dir : string
        Full path of dataset directory.
    history : dict
        Internal Dataset related history as dict.

    """
    dataset_id = dataset_dir.split("/")[-1]
    write_json(
        os.path.join(dataset_dir, "{0}_history.json".format(dataset_id)), history
    )


def update_csv(filename, data, delimiter=",", quotechar='"'):
    """Short summary.

    Parameters
    ----------
    filename : type
        Description of parameter `filename`.
    data : type
        Description of parameter `data`.
    delimiter : type
        Description of parameter `delimiter` (the default is ';').
    quotechar : type
        Description of parameter `quotechar` (the default is '"').

    Returns
    -------
    type
        Description of returned object.

    Raises
    -------
    ExceptionName
        Why the exception is raised.

    Examples
    -------
    Examples should be written in doctest format, and
    should illustrate how to use the function/class.
    >>>

    """
    tempfile = NamedTemporaryFile(mode="w", delete=False)

    with open(filename, "r") as csvfile, tempfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter, quotechar=quotechar)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(
            tempfile, fieldnames=fieldnames, delimiter=delimiter, quotechar=quotechar
        )
        # writer.writerow(fieldnames)
        writer.writeheader()
        for row in reader:
            ds_id = row["org.dataset_id"]
            if ds_id in data:
                for key, val in data[ds_id].items():
                    row[key] = val
            writer.writerow(row)
    shutil.move(tempfile.name, filename)


class History(object):
    """Doc."""

    def __init__(self, dataset_dir):
        """Short summary.

        Parameters
        ----------
        dataset_dir : type
            Description of parameter `dataset_dir`.

        Returns
        -------
        type
            Description of returned object.

        Raises
        -------
        ExceptionName
            Why the exception is raised.

        Examples
        -------
        Examples should be written in doctest format, and
        should illustrate how to use the function/class.
        >>>

        """
        self.filename = os.path.join(dataset_dir, "history.json")
        self.create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def __str__(self):
        """Return name of History() class for users.

        Returns
        -------
        string
            Naming of the History() class.

        """
        return "pyDataverse History class"
